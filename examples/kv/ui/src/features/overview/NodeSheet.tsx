import type { KvNode } from "../../api";
import { Field, Modal, Notice } from "../../components/ui";
import { UNKNOWN } from "../../format";
import { nodeName } from "../nodes";

/** All that the node said of itself in its last `INFO`, by name. */
export function NodeSheet({ node, onClose }: { node: KvNode; onClose: () => void }) {
  const fields = Object.entries(node.info).sort(([a], [b]) => a.localeCompare(b));
  return (
    <Modal title={nodeName(node)} sheet onClose={onClose}>
      {!node.up && <Notice tone="danger">{node.error ? `No answer: ${node.error}` : "No answer."} The fields are from the last answer.</Notice>}
      <dl aria-label="Fields" className="flex flex-col">
        <Field label="address">{node.addr}</Field>
        {fields.map(([name, value]) => (
          <Field key={name} label={name}>
            <span title={value}>{value || UNKNOWN}</span>
          </Field>
        ))}
      </dl>
    </Modal>
  );
}
