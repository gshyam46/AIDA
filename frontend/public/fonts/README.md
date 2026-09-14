# AIDA typography

AIDA self-hosts **Manrope** for interface text and **Newsreader** for editorial
headings. Fonts are served from `/fonts/`; browsers and production builds do not
contact Google Fonts, Fontsource, or a font CDN. The definitions live in
`app/fonts.css` and expose `--font-ui` and `--font-display`.

## Upstream sources and licenses

The unmodified WOFF2 assets and licenses were retrieved from the pinned
Fontsource packages below on 2026-09-15. Fontsource distributes the upstream
Google Fonts families. Both fonts use the SIL Open Font License 1.1; retain the
included license files with distributions.

| Family | Version | Package source | Upstream project | Included license |
| --- | --- | --- | --- | --- |
| Manrope | 5.3.0 | [@fontsource-variable/manrope](https://www.npmjs.com/package/@fontsource-variable/manrope/v/5.3.0) | [sharanda/manrope](https://github.com/sharanda/manrope) | [Manrope-OFL.txt](Manrope-OFL.txt) |
| Newsreader | 5.3.0 | [@fontsource-variable/newsreader](https://www.npmjs.com/package/@fontsource-variable/newsreader/v/5.3.0) | [productiontype/Newsreader](https://github.com/productiontype/Newsreader) | [Newsreader-OFL.txt](Newsreader-OFL.txt) |

Manrope includes variable weights 200–800. Newsreader includes variable weights
200–800 and optical sizes 6–72, in normal and italic styles. Latin and extended
Latin subsets are supplied, with CSS unicode ranges so browsers fetch only the
needed subset. Other scripts use the system fallback font. `font-display: swap`
keeps content visible during font loading. The normal Latin files are preloaded
in the root layout; italic and extended subsets load only when used.

Exact download paths are
`https://cdn.jsdelivr.net/npm/@fontsource-variable/{family}@5.3.0/files/{filename}`.
The license files came from the same pinned packages at `/LICENSE`.

## Asset checksums

SHA-256 hashes of the files shipped here:

| Filename | SHA-256 |
| --- | --- |
| `manrope-latin-wght-normal.woff2` | `a30ddcd349703aff7464c34bef3fffdff405ee50c113440d7c8693c02d210972` |
| `manrope-latin-ext-wght-normal.woff2` | `3911b66d9f2e005a4b989223405d0e5032619c668597ba467cc76a23c8fffcfb` |
| `newsreader-latin-standard-normal.woff2` | `6e4f2958c3a7c4a80acde4e5a679abe7e01bc1e30b92be3c7a8b696ef401d101` |
| `newsreader-latin-ext-standard-normal.woff2` | `45683de03de37187604102316c0b42c0cb2d8dc9c4140a20ad471c3148cc1278` |
| `newsreader-latin-standard-italic.woff2` | `5dfcd10d24af8c82927ba57f7983dd997f7b200594c09c35a1b1ed9fc4597506` |
| `newsreader-latin-ext-standard-italic.woff2` | `bb24f6648f75d909eb74535916015c97ef72a1344e532ec7bbaf3f80eb6f389f` |

The AIDA vector symbol in `components/BrandLogo.tsx` and `app/icon.svg` is a
project-native mark, independent of these font licenses. The symbol uses an
architectural A with a folded crossbar that suggests an open ledger. It shares
one silhouette across the navigation, authentication pages, product workspace,
and favicon.
