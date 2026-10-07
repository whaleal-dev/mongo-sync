import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

/**
 * 侧边栏结构对应手册章节。
 * 文档内容由 scripts/sync-docs.mjs 从仓库 doc/ 生成，这里的 id 就是那批文件名（README.md → index.md）。
 */
const sidebars: SidebarsConfig = {
  tutorialSidebar: [
    'index',
    'intro',
    {
      type: 'category',
      label: '同步概念',
      items: ['concepts/overview'],
    },
    {
      type: 'category',
      label: '入门',
      items: ['getting-started/quickstart'],
    },
    {
      type: 'category',
      label: '使用指南',
      items: [
        'guide/configuration',
        'guide/sync-modes',
        'guide/api',
        'guide/verify',
        'guide/operations',
      ],
    },
    {
      type: 'category',
      label: '参考',
      items: ['ARCHITECTURE'],
    },
  ],
};

export default sidebars;
