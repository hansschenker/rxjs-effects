import { defineConfig } from "vitepress";

export default defineConfig({
  title: "rxjs-effects",
  description: "Type algebra meets reactive execution",
  base: "/rxjs-effects/",
  themeConfig: {
    nav: [{ text: "Guide", link: "/" }, { text: "GitHub", link: "https://github.com/hansschenker/rxjs-effects" }],
    sidebar: [{ text: "Guide", items: [
      { text: "Introduction", link: "/" },
      { text: "Execution contract", link: "/execution" },
      { text: "API", link: "/api" },
      { text: "Roadmap", link: "/roadmap" }
    ] }]
  }
});
