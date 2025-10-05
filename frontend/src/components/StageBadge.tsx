import clsx from "clsx";

type Props = {
  name: string;
  status: string;
};

export default function StageBadge({ name, status }: Props) {
  return (
    <span
      className={clsx("badge", {
        completed: status === "completed",
        failed: status === "failed",
      })}
    >
      <strong>{name}</strong>
      <span>{status}</span>
    </span>
  );
}
