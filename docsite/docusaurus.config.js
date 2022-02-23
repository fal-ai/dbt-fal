// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Features & Labels',
  tagline: 'fal: run python with dbt',
  url: 'https://docs.fal.ai',
  baseUrl: '/',
  onBrokenLinks: 'warn',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/fal-logo.svg',
  organizationName: 'fal-ai', // Usually your GitHub org/user name.
  projectName: 'fal', // Usually your repo name.

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          routeBasePath: '/',
          sidebarPath: require.resolve('./sidebars.js'),
          // Please change this to your repo.
          editUrl: 'https://github.com/fal-ai/fal/tree/main/docsite/docs',
        },
        blog: {
          showReadingTime: true,
          // Please change this to your repo.
          editUrl:
            'https://github.com/fal-ai/fal/tree/main/docsite/docs',
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      navbar: {
        title: 'Features & Labels',
        logo: {
          alt: 'fal Logo',
          src: 'img/fal-logo.svg',
        },
        items: [
          {
            type: 'docSidebar',
            position: 'left',
            sidebarId: 'cli',
            label: 'fal CLI',
          },
          {
            type: 'doc',
            docId: 'Feature Store/overview',
            label: 'Feature Store',
          },
          {
            to: 'https://blog.fal.ai',
            label: 'Blog',
            position: 'left'
          },
          {
            to: 'https://www.notion.so/featuresandlabels/February-Roadmap-e6521e39514a4ed598745aa71167de6b',
            label: 'Roadmap',
            position: 'left'
          },
          {
            to: 'https://fal.ai',
            label: 'Home',
            position: 'right'
          },
          {
            href: 'https://github.com/fal-ai/fal',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Community',
            items: [
              {
                label: 'GitHub',
                href: 'https://github.com/fal-ai',
              },
              {
                label: 'Discord',
                href: 'https://discord.gg/Fyc9PwrccF',
              },
            ],
          },
          {
            title: 'More',
            items: [
              {
                label: 'fal',
                href: 'https://fal.ai',
              },
              {
                label: 'Blog',
                href: 'https://blog.fal.ai',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} Features & Labels, Inc. Built with Docusaurus.`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
    }),
};

module.exports = config;
