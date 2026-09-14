import type {Metadata} from 'next'
import './globals.css'
import './aida.css'
export const metadata: Metadata = {
  title: 'AIDA — Artificial Intelligence Data Analyst',
  description: 'Ask business questions in plain language. AIDA resolves your words to approved definitions, compiles read-only SQL in code, and shows exactly how every answer was produced.',
}
export default function RootLayout({children}: {children: React.ReactNode}) {
  return <html lang="en"><body>{children}<div className="watermark" aria-hidden="true">Crafted by Ghanashyam</div></body></html>
}
