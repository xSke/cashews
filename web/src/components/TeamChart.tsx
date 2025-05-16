"use client";

import { CartesianGrid, Line, LineChart, ReferenceLine, XAxis } from "recharts";
import {
  ChartConfig,
  ChartContainer,
  ChartLegend,
  ChartLegendContent,
} from "./ui/chart";

export interface TeamChartProps {
  data: { day: number; wins: number }[];
}

const chartConfig = {
  desktop: {
    label: "Desktop",
    color: "#2563eb",
  },
  mobile: {
    label: "Mobile",
    color: "#60a5fa",
  },
} satisfies ChartConfig;

export default function TeamChart(props: TeamChartProps) {
  return (
    <ChartContainer config={chartConfig} className="h-[300px] w-full">
      <LineChart data={props.data}>
        <CartesianGrid strokeDasharray="3 3" />
        <Line type="step" dataKey={"wins"} dot={false} />
        <ReferenceLine y={0} />

        <XAxis label="Day" />
      </LineChart>
    </ChartContainer>
  );
}
