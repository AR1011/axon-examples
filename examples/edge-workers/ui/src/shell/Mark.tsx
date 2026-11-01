/** The mark of this example: a bolt in a square. */
export function Mark({ size = 24 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 32 32" aria-hidden="true" className="shrink-0">
      <rect width="32" height="32" rx="7" className="fill-(--text-color-kumo-brand)" />
      <path d="M18 5 8 18h7l-1 9 10-13h-7z" fill="#fff" />
    </svg>
  );
}
