import { Link } from "@tanstack/react-router";
import ThemeToggle from "./ThemeToggle";

export default function Navbar() {
  return (
    <header className="bg-gray-100 dark:bg-gray-900 border-b border-gray-200 dark:border-gray-900">
      <div className="container mx-auto flex flex-row gap-4 items-center">
        <Link to={"/"} className="py-4 font-semibold mr-2">
          🍲 Free Cashews
        </Link>
        <Link to={"/teams"} className="py-4">
          Teams
        </Link>
        <Link to={"/players"} className="py-4">
          Players
        </Link>
        <Link to={"/map"} className="py-4">
          Map
        </Link>

        <div className="flex flex-1">&nbsp;</div>

        <ThemeToggle />
      </div>
    </header>
  );
}
