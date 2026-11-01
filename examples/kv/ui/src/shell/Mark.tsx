/** The mark of this example: four shards in a square. */
export function Mark({ size = 24 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 32 32" aria-hidden="true" className="shrink-0">
      <rect width="32" height="32" rx="7" className="fill-(--text-color-kumo-brand)" />
      <rect x="8" y="8" width="7" height="7" rx="1.5" fill="#fff" />
      <rect x="17" y="8" width="7" height="7" rx="1.5" fill="#fff" />
      <rect x="8" y="17" width="7" height="7" rx="1.5" fill="#fff" />
      <rect x="17" y="17" width="7" height="7" rx="1.5" fill="#fff" fillOpacity=".55" />
    </svg>
  );
}
