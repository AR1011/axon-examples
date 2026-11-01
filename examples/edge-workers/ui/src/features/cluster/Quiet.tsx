import { RUNNER } from "../../api";
import { Notice } from "../../components/ui";
import { clock } from "../../format";
import { useCluster } from "../workers/queries";

/** Says which nodes give nothing now: runners that did not answer, and nodes that were in the cluster and are not. */
export function Quiet({ runners = false, effect = "", className }: { runners?: boolean; effect?: string; className?: string }) {
  const cluster = useCluster();
  const silent = cluster.data?.silent ?? [];
  const left = (cluster.data?.gone ?? []).filter((g) => !runners || g.node.roles.includes(RUNNER));
  if (!silent.length && !left.length) return null;
  const what = runners ? "runners" : "nodes";
  return (
    <div className={className}>
      <Notice tone="warning">
        {silent.length > 0 && `These runners did not answer: ${silent.join(", ")}. `}
        {left.length > 0 && `These ${what} are not in the cluster now: ${left.map((g) => `${g.node.id} (from ${clock(g.since_ms, true)})`).join(", ")}. `}
        {effect}
      </Notice>
    </div>
  );
}
