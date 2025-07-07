import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function createSeasonList(currentSeason: number): number[] {
  const list: number[] = [];
  for (var i = currentSeason; i >= 0; i--) {
    list.push(i);
  }
  return list;
}
