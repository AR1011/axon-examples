/** mdpad's mark: an m and a down arrow, for Markdown, on a green square. */
export function Mark({ size = 24 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 32 32" aria-hidden="true" className="shrink-0">
      <rect width="32" height="32" rx="8" fill="#2f6f5e" />
      <path d="M8 22V10l4 5 4-5v12M20 10v12m-2-3 2 3 2-3" stroke="#fff" strokeWidth="2.2" fill="none" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}
