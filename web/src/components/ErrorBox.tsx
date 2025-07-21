import { ErrorComponentProps } from "@tanstack/react-router";

export default function ErrorBox(props: ErrorComponentProps) {
  return (
    <div className="rounded-md bg-red-800/20 border-2 border-red-800/50 p-4 text-sm">
      <pre className="font-semibold mb-1">
        Error: {props.error.message?.toString()}
      </pre>
      <pre>{props.error.stack}</pre>
      <pre>{JSON.stringify(props.error.cause)}</pre>
      <pre>{props.info?.componentStack ?? ""}</pre>
    </div>
  );
}
