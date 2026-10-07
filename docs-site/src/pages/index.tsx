import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';

import styles from './index.module.css';

const quickRoutes = [
  {
    title: '快速开始',
    description: '脚本 / Maven / 嵌入式 SDK 三种启动方式，先跑通再深入。',
    to: '/docs/getting-started/quickstart',
  },
  {
    title: '配置项全解',
    description: '全部配置键、默认值与启动校验规则。',
    to: '/docs/guide/configuration',
  },
  {
    title: '同步模式',
    description: '仅全量、全量∥增量、全量后追平再停、仅增量。',
    to: '/docs/guide/sync-modes',
  },
  {
    title: '运行与排障',
    description: '进度字段、告警、故障排查、已知限制与 FAQ。',
    to: '/docs/guide/operations',
  },
];

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  const bannerUrl = useBaseUrl('/img/banner.svg');
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <img className={styles.banner} src={bannerUrl} alt="mongo-sync" />
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link className="button button--secondary button--lg" to="/docs/">
            开始阅读手册
          </Link>
          <Link className="button button--info button--lg" to="/docs/intro">
            这是给谁用的
          </Link>
        </div>
      </div>
    </header>
  );
}

function QuickNavigation() {
  return (
    <section className={styles.quickRoutes} aria-labelledby="quick-routes-heading">
      <div className="container">
        <Heading as="h2" id="quick-routes-heading">
          按任务进入
        </Heading>
        <p className={styles.quickRoutesIntro}>
          MongoDB 文档同步 SDK：一套 API 或一条命令，完成全量 + 增量、DDL 跟随、多库表过滤与数据校验。
        </p>
        <div className={styles.quickRouteGrid}>
          {quickRoutes.map((route) => (
            <Link className={styles.quickRoute} key={route.to} to={route.to}>
              <Heading as="h3">{route.title}</Heading>
              <p>{route.description}</p>
            </Link>
          ))}
        </div>
      </div>
    </section>
  );
}

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={siteConfig.title}
      description="mongo-sync 使用手册：MongoDB 文档同步 SDK 的安装、配置、同步模式、SDK 与控制面、数据校验与排障。">
      <HomepageHeader />
      <main>
        <QuickNavigation />
      </main>
    </Layout>
  );
}
