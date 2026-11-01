/** The mark of this example: a path from a start to an end, in a square. */
export function Mark({ size = 24 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 32 32" aria-hidden="true" className="shrink-0">
      <rect width="32" height="32" rx="7" fill="#2563eb" />
      <path d="M9 10h7a4 4 0 0 1 0 8h-1a4 4 0 0 0 0 8h8" fill="none" stroke="#fff" strokeWidth="2.6" strokeLinecap="round" />
      <circle cx="9" cy="10" r="2.4" fill="#fff" />
      <circle cx="23" cy="26" r="2.4" fill="#fff" />
    </svg>
  );
}
