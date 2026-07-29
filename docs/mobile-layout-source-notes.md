# Mobile layout source notes

The first public beta pass borrowed its visual direction from the lab UI, but the
mobile reliability lesson came from the saved Auzietek Drupal tree:

- source: `/home/auzieman/Projects/auzietek/drupal/web/themes/contrib/drupal8_parallax_theme`
- libraries: Bootstrap plus `css/global.css` and `css/media.css`

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

Regression guard:

If a phone screenshot starts with mostly navigation/chrome and very little page
content, the layout has drifted away from the Auzietek/Drupal mobile pattern.
