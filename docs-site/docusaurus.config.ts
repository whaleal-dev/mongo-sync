import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

/** 公开文档站：https://docs.whaleal.com/mongo-sync/ */
const siteUrl = process.env.DOCS_SITE_URL ?? 'https://docs.whaleal.com';
const siteBaseUrl = process.env.DOCS_SITE_BASE_URL ?? '/mongo-sync/';

const config: Config = {
  title: 'mongo-sync 文档站',
  tagline: 'MongoDB 文档同步 SDK · 全量 + 增量 · DDL 跟随 · 多库表过滤',
  favicon: 'img/favicon.png',
  future: {v4: true},
  url: siteUrl,
  baseUrl: siteBaseUrl,
  organizationName: 'whaleal-dev',
  projectName: 'mongo-sync',
  onBrokenLinks: 'throw',

  // 手册是纯 GFM Markdown：正文里有 <version>、<hash> 这类占位符和原生 HTML 横幅。
  // 按 MDX 解析会把它们当成 JSX 表达式而构建失败，所以显式声明用 CommonMark。
  markdown: {format: 'md'},

  i18n: {
    defaultLocale: 'zh-Hans',
    locales: ['zh-Hans'],
  },
  presets: [
    [
      'classic',
      {
        docs: {
          routeBasePath: 'docs',
          sidebarPath: './sidebars.ts',
          // 站点内容由 scripts/sync-docs.mjs 从仓库 doc/ 生成，编辑入口指回真源
          editUrl: 'https://github.com/whaleal-dev/mongo-sync/tree/main/doc/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],
  themes: [
    [
      require.resolve('@easyops-cn/docusaurus-search-local'),
      {
        hashed: true,
        language: ['zh', 'en'],
        indexDocs: true,
        indexBlog: false,
        docsRouteBasePath: 'docs',
        searchResultLimits: 10,
        searchResultContextMaxLength: 80,
      },
    ],
  ],
  themeConfig: {
    navbar: {
      title: 'mongo-sync',
      logo: {
        alt: 'Whaleal',
        src: 'img/logo.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'tutorialSidebar',
          position: 'left',
          label: '使用手册',
        },
        {
          type: 'search',
          position: 'right',
        },
        {
          href: 'https://github.com/whaleal-dev/mongo-sync',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: '手册',
          items: [
            {label: '手册首页', to: '/docs/'},
            {label: '快速开始', to: '/docs/getting-started/quickstart'},
            {label: '配置项全解', to: '/docs/guide/configuration'},
            {label: '运行与排障', to: '/docs/guide/operations'},
          ],
        },
        {
          title: '资源',
          items: [
            {label: 'GitHub', href: 'https://github.com/whaleal-dev/mongo-sync'},
            {label: 'Issues', href: 'https://github.com/whaleal-dev/mongo-sync/issues'},
            {label: '仓库 README', href: 'https://github.com/whaleal-dev/mongo-sync#readme'},
            {label: '姊妹产品 rds-sync', href: 'https://docs.whaleal.com/rds-sync/'},
          ],
        },
      ],
      copyright: `Copyright (c) ${new Date().getFullYear()} whaleal-dev · 基于 Docusaurus 构建`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['java', 'bash', 'json', 'properties'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
