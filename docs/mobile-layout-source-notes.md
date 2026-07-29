# Mobile layout source notes

The first public beta pass borrowed its visual direction from the lab UI, but the
mobile reliability lesson came from the saved Auzietek Drupal tree:

- source: `/home/auzieman/Projects/auzietek/drupal/web/themes/contrib/drupal8_parallax_theme`
- libraries: Bootstrap plus `css/global.css` and `css/media.css`
- useful JS/plugin concepts: `js/custom.js`, SmartMenus, FlexSlider, Owl
  Carousel, WOW reveal animation

Useful patterns to preserve:

- Mobile should collapse to normal one-column document flow early.
- The desktop-side navigation can stay dramatic, but on phones it should become
  a compact top band, not a tall second header.
- Images, embeds, tables, and code blocks must cap at `max-width: 100%`; tables
  and code scroll horizontally instead of forcing page overflow.
- Avoid `background-attachment: fixed` on small screens. It is fragile on iOS and
  can make otherwise good layouts feel broken.
- Desktop can carry the parallax/glass styling. Mobile should keep the brand
  personality while letting content win.

## Bootstrap concepts to keep

We do not need to import Bootstrap to benefit from its layout instincts:

- Use named width rails:
  - shell/site width
  - wide content width
  - narrow/reading width
- Collapse to one column at tablet/mobile breakpoints.
- Keep gutters predictable and small.
- Put overflow responsibility on media/table/code blocks, not the page.
- Let mobile use full-width outer sections while constraining readable inner
  content.

## Parallax/theme concepts to keep

The old Drupal theme looked good because it combined simple content flow with a
few theatrical layers:

- fixed/cover hero backgrounds on desktop
- translucent panels over atmospheric backgrounds
- subtle reveal/settle motion
- carousel/card rhythm for grouped content
- smooth same-page anchor motion

Micro-blog should keep the mood without copying the plugin stack. The native
replacement is:

- CSS layered backgrounds and glass panels
- reduced-motion-aware reveal animation
- active mobile nav chip centering
- smooth anchor scrolling
- no fixed background attachment on small screens

Avoid reintroducing jQuery-era dependencies unless a feature genuinely needs
them. The goal is the feel, not the old machinery.

Regression guard:

If a phone screenshot starts with mostly navigation/chrome and very little page
content, the layout has drifted away from the Auzietek/Drupal mobile pattern.

Responsive shell guard:

- Keep the document itself bounded: `html`, `body`, and the main frame should
  stay at `width/max-width: 100%`.
- Prefer `minmax(0, 1fr)` and `minmax(min(100%, 18rem), 1fr)` in grids.
- Flex and grid children should be allowed to shrink with `min-width: 0`.
- Imported article HTML is hostile until proven otherwise; constrain images,
  SVG, canvas, video, embeds, iframes, tables, code, and long links near the
  `.article-body` boundary.
- Do not use `overflow-x: hidden` as the primary fix. It can hide the symptom,
  but the real fix is identifying which child is wider than the viewport.
- Auzietek's left rail should survive desktop browser resizing. If a desktop
  window is squeezed below the comfortable layout width, reserve a sane content
  column and let the document scroll rather than crushing the article into a
  receipt.
- Actual mobile/touch layouts should collapse into the compact top rail so the
  content column remains the visual anchor.

Browser-console overflow check:

```js
[...document.querySelectorAll("*")].filter((el) => {
  const rect = el.getBoundingClientRect();
  return rect.right > document.documentElement.clientWidth + 1;
}).map((el) => ({
  tag: el.tagName,
  className: el.className,
  id: el.id,
  right: Math.round(el.getBoundingClientRect().right),
  width: Math.round(el.getBoundingClientRect().width),
}));
```
