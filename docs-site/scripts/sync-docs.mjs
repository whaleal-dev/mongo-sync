#!/usr/bin/env node
/**
 * 把仓库里的 Markdown 手册同步成 Docusaurus 可消费的 docs-site/docs/ 目录。
 *
 * ⚠️ docs-site/docs/ 与 docs-site/static/img/banner.svg 都是**生成物，不要手改**。
 *    真源只有一份：mongo-sync 的 `doc/`（本仓）/ rds-sync 的 `docs/`。
 *    `npm start` / `npm run build` 前会自动跑（prestart / prebuild），CI 里同样如此。
 *
 * 为什么需要这个脚本，而不是像 quick-sms 那样把文档复制一份进 docs-site/：
 *   1. 复制一份 = 同一篇文档两处维护，改一处忘一处就会出现「站上和仓库不一致」；
 *   2. 手册里大量链接指向文档树之外（../README.md、../bin/README.md、examples/、oplog/、
 *      ../mongo-transfer-model/README.md…），这些文件不在站点里，而站点用
 *      onBrokenLinks: 'throw'，直接照搬会让构建失败；
 *   3. README.md 在 Docusaurus 里不会自动成为文档首页，需要改成 index.md。
 *
 * 环境变量（都有默认值，CI 里由 workflow 传入）：
 *   DOCS_REPO           GitHub 仓库名，默认 mongo-sync
 *   DOCS_SRC            手册所在目录（相对仓库根），默认 doc
 *   DOCS_BRANCH         生成 GitHub 链接用的分支，默认 main
 *   DOCS_SITE_BASE_URL  站点 baseUrl，默认 /mongo-sync/
 */

import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const siteRoot = path.resolve(here, '..');
const repoRoot = path.resolve(siteRoot, '..');

const REPO = process.env.DOCS_REPO ?? 'mongo-sync';
const SRC_DIR = process.env.DOCS_SRC ?? 'doc';
const BRANCH = process.env.DOCS_BRANCH ?? 'main';
const BASE_URL = `${(process.env.DOCS_SITE_BASE_URL ?? `/mongo-sync/`).replace(/\/+$/, '')}/`;
const GITHUB = `https://github.com/whaleal-dev/${REPO}`;
const RAW = `https://raw.githubusercontent.com/whaleal-dev/${REPO}/${BRANCH}`;

/** 要同步进站点的文档（相对 SRC_DIR）。没列出的文件不进站点，链接会被改写成 GitHub 地址。 */
const INCLUDE_DOCS = [
  'README.md',
  'intro.md',
  'concepts/overview.md',
  'getting-started/quickstart.md',
  'guide/configuration.md',
  'guide/sync-modes.md',
  'guide/api.md',
  'guide/verify.md',
  'guide/operations.md',
  'ARCHITECTURE.md',
];

/** 需要一并拷进站点静态目录的资源：源路径（相对 SRC_DIR）→ static 下目标路径。 */
const ASSETS = {
  'assets/banner.svg': 'img/banner.svg',
};

const DOC_INDEX = 'README.md'; // 源里的索引页
const SITE_INDEX = 'index.md'; // 站点里的首页文档名

const srcRoot = path.join(repoRoot, SRC_DIR);
const outDocs = path.join(siteRoot, 'docs');
const outStatic = path.join(siteRoot, 'static');

/** 源里的相对路径 → 站点里的相对路径；不在站点里则返回 null。 */
function toSitePath(rel) {
  if (rel === DOC_INDEX) {
    return INCLUDE_DOCS.includes(DOC_INDEX) ? SITE_INDEX : null;
  }
  return INCLUDE_DOCS.includes(rel) ? rel : null;
}

function exists(rel) {
  try {
    return fs.statSync(path.join(repoRoot, rel));
  } catch {
    return null;
  }
}

/** 站点外的目标：文件给 blob，目录给 tree，图片给 raw。 */
function githubUrl(rel, {image = false} = {}) {
  const repoRel = path.posix.join(SRC_DIR, rel);
  const stat = exists(repoRel);
  const encoded = repoRel.split('/').map(encodeURIComponent).join('/');
  if (image) return `${RAW}/${encoded}`;
  if (stat?.isDirectory()) return `${GITHUB}/tree/${BRANCH}/${encoded}`;
  return `${GITHUB}/blob/${BRANCH}/${encoded}`;
}

const stats = {docs: 0, internal: 0, external: 0, images: 0, warnings: []};

/** 改写单个链接目标。 */
function rewriteTarget(target, srcRel, {image = false} = {}) {
  if (/^(https?:|mailto:|tel:)/i.test(target)) return target;

  const hashAt = target.indexOf('#');
  const rawPath = hashAt === -1 ? target : target.slice(0, hashAt);
  const anchor = hashAt === -1 ? '' : target.slice(hashAt);
  if (rawPath === '') return target; // 纯锚点，原样保留

  const abs = path.posix.normalize(path.posix.join(path.posix.dirname(srcRel), rawPath));

  // 落在站点外的（../xxx）
  if (abs.startsWith('..')) {
    stats.external += 1;
    return `${githubUrl(abs, {image})}${image ? '' : anchor}`;
  }

  const siteTarget = toSitePath(abs);
  if (siteTarget) {
    const fromDir = path.posix.dirname(toSitePath(srcRel) ?? srcRel);
    const rel = path.posix.relative(fromDir, siteTarget) || path.posix.basename(siteTarget);
    stats.internal += 1;
    return `${rel}${anchor}`;
  }

  // 站点内有这个文件，但不在同步清单里 → 也是 GitHub 链接
  if (!exists(path.posix.join(SRC_DIR, abs))) {
    stats.warnings.push(`${srcRel}: 链接目标不存在 -> ${rawPath}`);
    return target;
  }
  stats.external += 1;
  return `${githubUrl(abs, {image})}${image ? '' : anchor}`;
}

function transform(markdown, srcRel) {
  let out = markdown;

  // 1) Markdown 链接与图片：[text](target) / ![alt](target)
  out = out.replace(/(!?\[[^\]]*\]\()([^)\s]+)(\))/g, (whole, open, target, close) => {
    const image = open.startsWith('!');
    if (image) stats.images += 1;
    return `${open}${rewriteTarget(target, srcRel, {image})}${close}`;
  });

  // 2) 正文里的 HTML <img src>（目前只有手册索引页的 banner）
  out = out.replace(/(<img\b[^>]*?\bsrc=")([^"]+)(")/g, (whole, pre, src, post) => {
    const mapped = ASSETS[path.posix.normalize(src)];
    if (!mapped) return whole;
    return `${pre}${BASE_URL}${mapped}${post}`;
  });

  return out;
}

function main() {
  if (!fs.existsSync(srcRoot)) {
    throw new Error(`手册目录不存在：${srcRoot}`);
  }

  fs.rmSync(outDocs, {recursive: true, force: true});
  fs.mkdirSync(outDocs, {recursive: true});

  for (const rel of INCLUDE_DOCS) {
    const from = path.join(srcRoot, rel);
    if (!fs.existsSync(from)) {
      throw new Error(`同步清单里的文件不存在：${SRC_DIR}/${rel}`);
    }
    const toRel = toSitePath(rel);
    const to = path.join(outDocs, toRel);
    fs.mkdirSync(path.dirname(to), {recursive: true});
    const source = fs.readFileSync(from, 'utf8');
    fs.writeFileSync(to, transform(source, rel), 'utf8');
    stats.docs += 1;
  }

  for (const [src, dest] of Object.entries(ASSETS)) {
    const from = path.join(srcRoot, src);
    if (!fs.existsSync(from)) {
      throw new Error(`同步清单里的资源不存在：${SRC_DIR}/${src}`);
    }
    const to = path.join(outStatic, dest);
    fs.mkdirSync(path.dirname(to), {recursive: true});
    fs.copyFileSync(from, to);
  }

  console.log(
    `[sync-docs] ${SRC_DIR}/ → docs-site/docs/ ：${stats.docs} 篇文档，` +
      `站内链接 ${stats.internal} 个，改写为 GitHub 绝对地址 ${stats.external} 个，` +
      `图片 ${stats.images} 个，资源 ${Object.keys(ASSETS).length} 个`,
  );
  for (const w of stats.warnings) console.warn(`[sync-docs] ⚠️ ${w}`);
}

main();
