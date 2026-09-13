// Build-time switch for the public landing deployment: marketing and benchmark pages only, no accounts or data API.
export const PREVIEW = process.env.NEXT_PUBLIC_AIDA_MODE === 'preview'
