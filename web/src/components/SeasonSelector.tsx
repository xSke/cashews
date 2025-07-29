import { useQuery } from "@tanstack/react-query";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "./ui/select";
import { timeQuery } from "@/lib/data";
import { createSeasonList } from "@/lib/utils";

export interface SeasonSelectorProps {
  season: number;
  setSeason: (season: number) => void;
}

export default function SeasonSelector(props: SeasonSelectorProps) {
  const time = useQuery(timeQuery);
  const currentSeason = time.data?.data?.season_number ?? 0;

  const seasons = createSeasonList(currentSeason);

  return (
    <Select
      value={props.season.toString()}
      onValueChange={(val) => {
        props.setSeason(parseInt(val, 10) ?? 0);
      }}
    >
      <SelectTrigger className="w-[180px]">
        <SelectValue placeholder="Season..."></SelectValue>
      </SelectTrigger>
      <SelectContent>
        {seasons.map((s) => (
          <SelectItem value={s.toString()}>Season {s}</SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}
