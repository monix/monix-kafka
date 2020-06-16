const repoUrl = "https://github.com/monix/monix-kafka";

const apiUrl = "api/monix/kafka/index.html"

const siteConfig = {
  title: "Monix Kafka",
  tagline: "A Monix integration with Kafka.",
  url: "https://monix.github.io/monix-kafka",
  baseUrl: "/monix-kafka/",
  cname: "monix.github.io/monix-kafka",

  customDocsPath: "monix-kafka-docs/target/mdoc",

  projectName: "monix-kafka",
  organizationName: "monix",

  headerLinks: [
    { href: apiUrl, label: "API Docs" },
    { doc: "overview", label: "Documentation" },
    { href: repoUrl, label: "GitHub" }
  ],

  headerIcon: "img/monix-logo.svg",
  titleIcon: "img/monix-logo.svg",
  favicon: "img/monix-logo.png",

  colors: {
    primaryColor: "#122932",
    secondaryColor: "#153243"
  },

  copyright: `Copyright © 2014-${new Date().getFullYear()} The Monix Developers.`,

  highlight: { theme: "github" },

  onPageNav: "separate",

  separateCss: ["api"],

  cleanUrl: true,

  repoUrl,

  apiUrl
};

module.exports = siteConfig;
