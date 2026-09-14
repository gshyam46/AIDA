import styles from './BrandLogo.module.css'

type BrandLogoProps = {
  className?: string
  compact?: boolean
}

/** Shared AIDA signature. The folded crossbar suggests an open ledger. */
export default function BrandLogo({className = '', compact = false}: BrandLogoProps) {
  return (
    <span className={`${styles.logo} ${className}`}>
      <svg className={styles.symbol} viewBox="0 0 40 40" fill="none" aria-hidden="true" focusable="false">
        <path d="M6 33 18 7h4l12 26h-6L20 14 12 33H6Z" fill="currentColor"/>
        <path d="m13 24 7-2 7 2v4l-7-2-7 2v-4Z" fill="currentColor"/>
      </svg>
      <span className={compact ? styles.visuallyHidden : styles.wordmark}>AIDA</span>
    </span>
  )
}
