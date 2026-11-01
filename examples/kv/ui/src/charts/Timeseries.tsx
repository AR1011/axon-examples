import { ChartLegend, TimeseriesChart } from "@cloudflare/kumo";
import { LineChart } from "echarts/charts";
import { AriaComponent, BrushComponent, GridComponent, MarkLineComponent, ToolboxComponent, TooltipComponent } from "echarts/components";
import * as echarts from "echarts/core";
import { CanvasRenderer } from "echarts/renderers";
import { clock, perSecond } from "../format";
import { useTheme } from "../hooks";

// What the library's chart draws with.
echarts.use([LineChart, GridComponent, TooltipComponent, BrushComponent, AriaComponent, MarkLineComponent, ToolboxComponent, CanvasRenderer]);

/**
 * On a narrow chart the time labels do not all fit: those that would touch another are not drawn. The library does not
 * give this option, so it goes on each chart the library makes, after the library's own options.
 */
function hideOverlap(chart: echarts.ECharts | null) {
  if (!chart) return;
  queueMicrotask(() => {
    if (!chart.isDisposed()) chart.setOption({ xAxis: { axisLabel: { hideOverlap: true } } });
  });
}

export interface Line {
  name: string;
  points: [number, number | null][];
  /** For the mode the page is in. */
  color: (dark: boolean) => string;
}

/** Lines of rates over time, with a legend that gives each line's last value. */
export default function Timeseries({ lines, label }: { lines: Line[]; label: string }) {
  const [theme] = useTheme();
  const dark = theme === "dark";
  // A null stays in the data: the chart leaves a gap there, so a node that did not answer is not drawn as a line.
  const data = lines.map((line) => ({ name: line.name, color: line.color(dark), data: line.points as [number, number][] }));
  // Under five minutes of points the ticks are less than a minute apart: the labels then need the seconds.
  const times = lines.flatMap((line) => line.points.filter((p) => p[1] != null).map((p) => p[0]));
  const short = times.length > 0 && Math.max(...times) - Math.min(...times) < 300_000;
  return (
    <div className="flex flex-col gap-1 px-2 pt-3 pb-3">
      <div>
        <TimeseriesChart
          ref={hideOverlap}
          echarts={echarts as never}
          data={data}
          height={200}
          isDarkMode={dark}
          xAxisTickCount={short ? 3 : 5}
          xAxisTickFormat={(ms) => clock(ms, short)}
          yAxisTickFormat={(n) => perSecond(n)}
          tooltipValueFormat={(n) => perSecond(n)}
          ariaDescription={label}
        />
      </div>
      <div className="flex min-h-5 flex-wrap gap-x-4 gap-y-1 px-2">
        {lines.map((line, i) => {
          const last = line.points.length ? line.points[line.points.length - 1][1] : null;
          return <ChartLegend.SmallItem key={line.name} name={line.name} color={data[i].color} value={perSecond(last)} />;
        })}
      </div>
    </div>
  );
}
