import { ChartLegend, TimeseriesChart } from "@cloudflare/kumo";
import { LineChart } from "echarts/charts";
import { AriaComponent, BrushComponent, GridComponent, MarkLineComponent, ToolboxComponent, TooltipComponent } from "echarts/components";
import * as echarts from "echarts/core";
import { CanvasRenderer } from "echarts/renderers";
import { clock, count, micros, perSecond } from "../format";
import { useTheme } from "../hooks";

// What the library's chart draws with.
echarts.use([LineChart, GridComponent, TooltipComponent, BrushComponent, AriaComponent, MarkLineComponent, ToolboxComponent, CanvasRenderer]);

export type Unit = "/s" | "us" | "count";

export interface Line {
  name: string;
  points: [number, number | null][];
  /** For the mode the page is in. */
  color: (dark: boolean) => string;
}

const byUnit = (unit: Unit, n: number) => (unit === "/s" ? perSecond(n) : unit === "us" ? micros(n) : count(n));

export default function Timeseries({ lines, unit, label }: { lines: Line[]; unit: Unit; label: string }) {
  const [theme] = useTheme();
  const dark = theme === "dark";
  const data = lines.map((line) => ({ name: line.name, color: line.color(dark), data: line.points.filter((p): p is [number, number] => p[1] != null) }));
  // Under five minutes of points the ticks are less than a minute apart: the labels then need the seconds.
  const times = data.flatMap((line) => line.data.map((p) => p[0]));
  const short = times.length > 0 && Math.max(...times) - Math.min(...times) < 300_000;
  return (
    <div className="flex flex-col gap-1 px-2 pt-3 pb-3">
      <div role="img" aria-label={label}>
        <TimeseriesChart
          echarts={echarts as never}
          data={data}
          height={200}
          isDarkMode={dark}
          xAxisTickCount={short ? 3 : 5}
          xAxisTickFormat={(ms) => clock(ms, short)}
          yAxisTickFormat={(n) => byUnit(unit, n)}
          tooltipValueFormat={(n) => byUnit(unit, n)}
          ariaDescription={label}
        />
      </div>
      <div className="flex flex-wrap gap-x-4 gap-y-1 px-2">
        {data.map((line) => (
          <ChartLegend.SmallItem key={line.name} name={line.name} color={line.color} value={line.data.length ? byUnit(unit, line.data[line.data.length - 1][1]) : "—"} />
        ))}
      </div>
    </div>
  );
}
