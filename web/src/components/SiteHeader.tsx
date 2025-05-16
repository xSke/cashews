import ThemeToggle from "./ThemeToggle";

export default function SideHeader() {
  return (
    <header className="bg-gray-100 dark:bg-gray-900 border-b border-gray-200 dark:border-gray-900">
      <div className="container mx-auto px-4 flex flex-row gap-4 items-center">
        <a className="py-4 font-semibold mr-4">Site</a>
        <a className="py-4">Teams</a>
        <a className="py-4">Players</a>
        <a className="py-4">Map</a>

        <div className="flex flex-1">&nbsp;</div>

        <a className="py-4">Right</a>
        <ThemeToggle />
      </div>
    </header>
  );
}
