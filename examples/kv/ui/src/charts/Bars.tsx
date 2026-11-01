import { Chart, ChartLegend, ChartPalette } from "@cloudflare/kumo";
import { BarChart } from "echarts/charts";
import { AriaComponent, GridComponent, TooltipComponent } from "echarts/components";
import * as echarts from "echarts/core";
import { CanvasRenderer } from "echarts/renderers";
import { count, whole } from "../format";
import { useTheme } from "../hooks";

echarts.use([BarChart, GridComponent, TooltipComponent, AriaComponent, CanvasRenderer]);

export interface Bars {
  /** The groups along the axis. */
  groups: string[];
  /** One bar in each group per series; null where the run has no value. */
  series: { name: string; values: (number | null)[] }[];
}

/** Groups of bars, one colour per series, with a legend below. */
export default function Bars({ bars, label, unit }: { bars: Bars; label: string; unit: string }) {
  const [theme] = useTheme();
  const dark = theme === "dark";
  const text = ChartPalette.text("primary", dark);
  // A long list of groups lies along the vertical axis, so each name has a line of its own on a phone too.
  const options = {
    aria: { enabled: true, label: { description: label } },
    grid: { left: 8, right: 16, top: 8, bottom: 8, containLabel: true },
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" }, valueFormatter: (n: number) => `${whole(n)} ${unit}` },
    xAxis: { type: "value", axisLabel: { color: text, formatter: (n: number) => count(n), hideOverlap: true }, splitLine: { lineStyle: { color: dark ? "#333" : "#eee" } } },
    yAxis: { type: "category", data: bars.groups, inverse: true, axisLabel: { color: text, fontFamily: "ui-monospace, monospace" }, axisTick: { show: false } },
    series: bars.series.map((s, i) => ({ type: "bar", name: s.name, data: s.values, color: ChartPalette.categorical(i, dark), barMaxWidth: 10 })),
  };
  const height = Math.max(160, bars.groups.length * (bars.series.length * 10 + 18) + 40);
  return (
    <div className="flex flex-col gap-2 px-2 pt-3 pb-3">
      <div>
        <Chart echarts={echarts as never} options={options as never} height={height} isDarkMode={dark} optionUpdateBehavior={{ notMerge: true }} />
      </div>
      <div className="flex flex-wrap gap-x-4 gap-y-1 px-2">
        {bars.series.map((s, i) => (
          <ChartLegend.SmallItem key={s.name} name={s.name} color={ChartPalette.categorical(i, dark)} value="" />
        ))}
      </div>
    </div>
  );
}
