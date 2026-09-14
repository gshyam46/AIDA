import type {Metadata, Viewport} from 'next'
import './fonts.css'
import './globals.css'
import './aida.css'
import './theme.css'
import './motion.css'
import './product-motion.css'
export const metadata: Metadata = {
  title: 'AIDA — A clearer view of your business',
  description: 'Ask business questions in plain language, explore the answers, and create dashboards with a clear view of the data behind every result.',
}

export const viewport: Viewport = {
  themeColor: '#FBFAF4',
  colorScheme: 'light',
}

export default function RootLayout({children}: {children: React.ReactNode}) {
  return (
    <html lang="en">
      <head>
        <link rel="preload" href="/fonts/manrope-latin-wght-normal.woff2" as="font" type="font/woff2" crossOrigin="anonymous"/>
        <link rel="preload" href="/fonts/newsreader-latin-standard-normal.woff2" as="font" type="font/woff2" crossOrigin="anonymous"/>
      </head>
      <body>
        {children}
        <div className="watermark" aria-hidden="true">Crafted by Ghanashyam</div>
      </body>
    </html>
  )
}
