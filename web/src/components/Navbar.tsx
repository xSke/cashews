import { Link, useMatchRoute } from "@tanstack/react-router";
import ThemeToggle from "./ThemeToggle";
import { useSidebar } from "./ui/sidebar";
import { Button } from "./ui/button";
import { cn } from "@/lib/utils";
import {
  MapIcon,
  MenuIcon,
  PersonStandingIcon,
  RefreshCcw,
  Trash,
  VolleyballIcon,
} from "lucide-react";
import { useIsMobile } from "@/hooks/use-mobile";
import { Drawer, DrawerContent, DrawerTrigger } from "./ui/drawer";
import clsx from "clsx";
import React from "react";
import { Tooltip, TooltipContent, TooltipTrigger } from "./ui/tooltip";
import { asyncStoragePersister } from "@/router";

function ClearCacheButton() {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Button
          variant="ghost"
          size="icon"
          onClick={async () => {
            console.log("refreshing");
            await asyncStoragePersister.removeClient();
            window.location.reload();
          }}
          className="cursor-pointer"
        >
          <RefreshCcw />
          {/* <Sun className="h-[1.2rem] w-[1.2rem] rotate-0 scale-100 transition-all dark:-rotate-90 dark:scale-0" /> */}
          {/* <Moon className="absolute h-[1.2rem] w-[1.2rem] rotate-90 scale-0 transition-all dark:rotate-0 dark:scale-100" /> */}
          <span className="sr-only">Clear cache</span>
        </Button>
      </TooltipTrigger>
      <TooltipContent>
        Clear cache (or free cache-ws, if you will)
      </TooltipContent>
    </Tooltip>
  );
}

function SidebarTrigger({
  className,
  onClick,
  ...props
}: React.ComponentProps<typeof Button>) {
  const { toggleSidebar } = useSidebar();

  return (
    <button
      data-sidebar="trigger"
      data-slot="sidebar-trigger"
      variant="ghost"
      className="mx-4"
      onClick={(event) => {
        onClick?.(event);
        toggleSidebar();
      }}
      {...props}
    >
      <MenuIcon className="size-5 mx-0" />
      <span className="sr-only">Toggle Sidebar</span>
    </button>
  );
}

export default function Navbar() {
  const isMobile = useIsMobile();

  const [open, setOpen] = React.useState(false);

  const matchRoute = useMatchRoute();
  const pages = [
    { text: "Teams", to: "/teams", icon: VolleyballIcon },
    { text: "Players", to: "/players", icon: PersonStandingIcon },
    { text: "Map", to: "/map", icon: MapIcon },
  ];

  return (
    <header className="bg-gray-100 dark:bg-gray-900 border-b border-gray-200 dark:border-gray-900 flex flex-row">
      <div className="container mx-auto flex flex-row gap-4 items-center pr-2">
        {isMobile && (
          <Drawer direction="left" open={open} onOpenChange={setOpen}>
            <DrawerTrigger asChild className="cursor-pointer">
              <button className="ml-4">
                <MenuIcon size={24} />
              </button>
            </DrawerTrigger>
            <DrawerContent>
              <DrawerContent>
                <nav className="flex flex-col py-2">
                  {pages.map((page) => {
                    const matches = !!matchRoute({ to: page.to });
                    return (
                      <Link
                        to={page.to}
                        className={clsx(
                          "px-4 py-2 hover:underline hover:bg-muted",
                          matches && "bg-gray-200 dark:bg-gray-700"
                        )}
                        onClick={() => {
                          setOpen(false);
                        }}
                      >
                        {page.text}
                      </Link>
                    );
                  })}
                </nav>
              </DrawerContent>
            </DrawerContent>
          </Drawer>
        )}
        <Link to={"/"} className="py-4 font-semibold mr-2">
          🍲 Free Cashews
        </Link>

        {!isMobile &&
          pages.map((page) => {
            return (
              <Link to={page.to} key={page.to} className="py-4">
                {page.text}
              </Link>
            );
          })}

        <div className="flex flex-1">&nbsp;</div>

        <span className="flex gap-1">
          <ClearCacheButton />
          <ThemeToggle />
        </span>
      </div>
    </header>
  );
}
