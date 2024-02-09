import { formatDate, getTableColumn } from "./util";
import { Table, TickerFormat, ChartVisualizationData, ChartVisualization } from "../types";

export async function prepareChartData(
  table: Table,
  visualization: ChartVisualization
): Promise<ChartVisualizationData> {
  if (table.body.rows.length === 0) return emptyVisualizationData(visualization);

  const aggregate = aggregateData(table, visualization);
  return createVisualizationData(visualization, aggregate);
}

function createVisualizationData(
  visualization: ChartVisualization,
  aggregate: Record<string, PrepareAggregatedData>
): ChartVisualizationData {
  const visualizationData = emptyVisualizationData(visualization);

  for (const aggdata of Object.values(aggregate)) {
    for (const group of Object.keys(aggdata.values)) {
      if (visualizationData.yKeys[group] === undefined) {
        visualizationData.yKeys[group] = {
          label: group,
          secondAxis: aggdata.secondAxis,
          tickerFormat: aggdata.tickerFormat,
        };
      }
    }
  }

  visualizationData.data = Object.values(aggregate)
    .sort((a: any, b: any) => (a.sortBy < b.sortBy ? -1 : b.sortBy < a.sortBy ? 1 : 0))
    .map((d) => {
      for (const key of Object.keys(d.values)) d.values[key] = Math.round(d.values[key] * 100) / 100;

      return {
        ...d.values,
        [d.xLabel]: d.xValue,
        __rowIds: d.rowIds,
        __sortBy: d.sortBy,
      };
    });

  return visualizationData;
}

function emptyVisualizationData(visualization: ChartVisualization): ChartVisualizationData {
  return {
    type: visualization.type,
    xKey: {
      label: visualization.group.label !== undefined ? visualization.group.label : visualization.group.column,
    },
    yKeys: {},
    data: [],
  };
}

function aggregateData(table: Table, visualization: ChartVisualization): Record<string, PrepareAggregatedData> {
  const aggregate: Record<string, PrepareAggregatedData> = {};

  const { groupBy, xSortable } = prepareX(table, visualization);
  const rowIds = table.body.rows.map((row) => row.id);
  const xLabel = visualization.group.label !== undefined ? visualization.group.label : visualization.group.column;

  const anyAddZeroes = visualization.values.some((value) => value.addZeroes === true);
  if (anyAddZeroes && xSortable != null) {
    for (const [uniqueValue, sortby] of Object.entries(xSortable)) {
      aggregate[uniqueValue] = {
        sortBy: sortby,
        rowIds: {},
        xLabel,
        xValue: uniqueValue,
        values: {},
        secondAxis: false,
        tickerFormat: "default",
      };
    }
  }

  for (const value of visualization.values) {
    // loop over all y values

    const aggFun = value.aggregate !== undefined ? value.aggregate : "count";
    let tickerFormat: TickerFormat = "default";
    if (aggFun === "pct" || aggFun === "count_pct") tickerFormat = "percent";

    const yValues = getTableColumn(table, value.column);
    if (yValues.length === 0) throw new Error(`Y column ${table.id}.${value.column} not found`);

    // If group_by column is specified, the columns in the aggregated data will be the unique group_by columns
    const yGroup = value.group_by !== undefined ? getTableColumn(table, value.group_by) : null;

    // if missing values should be treated as zero, we need to add the missing values after knowing all groups
    const addZeroes = value.addZeroes ?? false;
    const groupSummary: Record<string, { n: number; sum: number }> = {};

    for (let i = 0; i < rowIds.length; i++) {
      // loop over rows of table
      const xValue = groupBy[i];

      if (visualization.group.range !== undefined) {
        if (Number(xValue) < visualization.group.range[0] || Number(xValue) > visualization.group.range[1]) {
          continue;
        }
      }

      // SHOULD GROUP BE IGNORED IF NOT IN group.levels? MAYBE NOT, BECAUSE
      // THIS COULD HARM INFORMED CONSENT IF THE RESEARCHER IS UNAWARE OF CERTAIN GROUPS
      // if (visualization.group.levels !== undefined) {
      //   // formatLevels has xSortable < 0 if no match with levels
      //   if (xSortable !== null && xSortable[i] < 0) continue
      // }

      const yValue = yValues[i];
      const label = value.label !== undefined ? value.label : value.column;
      const group = yGroup != null ? yGroup[i] : label;

      const sortBy = xSortable != null ? xSortable[xValue] : groupBy[i];

      // calculate group summary statistics. This is used for the mean, pct and count_pct aggregations
      if (groupSummary[group] === undefined) groupSummary[group] = { n: 0, sum: 0 };
      if (aggFun === "count_pct" || aggFun === "mean") groupSummary[group].n += 1;
      if (aggFun === "pct") groupSummary[group].sum += Number(yValue) ?? 0;

      if (aggregate[xValue] === undefined) {
        aggregate[xValue] = {
          sortBy: sortBy,
          rowIds: {},
          xLabel,
          xValue: String(xValue),
          values: {},
          secondAxis: value.secondAxis,
          tickerFormat,
        };
      }
      if (aggregate[xValue].rowIds[group] === undefined) aggregate[xValue].rowIds[group] = [];
      aggregate[xValue].rowIds[group].push(rowIds[i]);

      if (aggregate[xValue].values[group] === undefined) aggregate[xValue].values[group] = 0;
      if (aggFun === "count" || aggFun === "count_pct") aggregate[xValue].values[group] += 1;
      if (aggFun === "sum" || aggFun === "mean" || aggFun === "pct") {
        aggregate[xValue].values[group] += Number(yValue) ?? 0;
      }
    }

    // use groupSummary to calculate the mean, pct and count_pct aggregations
    Object.keys(groupSummary).forEach((group) => {
      for (const xValue of Object.keys(aggregate)) {
        if (aggregate[xValue].values[group] === undefined) {
          if (addZeroes) aggregate[xValue].values[group] = 0;
          else continue;
        }
        if (aggFun === "mean") {
          aggregate[xValue].values[group] = Number(aggregate[xValue].values[group]) / groupSummary[group].n;
        }
        if (aggFun === "count_pct") {
          aggregate[xValue].values[group] = (100 * Number(aggregate[xValue].values[group])) / groupSummary[group].n;
        }
        if (aggFun === "pct") {
          aggregate[xValue].values[group] = (100 * Number(aggregate[xValue].values[group])) / groupSummary[group].sum;
        }
      }
    });
  }

  return aggregate;
}

function prepareX(
  table: Table,
  visualization: ChartVisualization
): { groupBy: string[]; xSortable: Record<string, string | number> | null } {
  let groupBy = getTableColumn(table, visualization.group.column);
  if (groupBy.length === 0) {
    throw new Error(`X column ${table.id}.${visualization.group.column} not found`);
  }
  // let xSortable: Array<string | number> | null = null // separate variable allows using epoch time for sorting dates
  let xSortable: Record<string, string | number> | null = null; // map x values to sortable values

  // ADD CODE TO TRANSFORM TO DATE, BUT THEN ALSO KEEP AN INDEX BASED ON THE DATE ORDER
  if (visualization.group.dateFormat !== undefined) {
    [groupBy, xSortable] = formatDate(groupBy, visualization.group.dateFormat);
  }

  if (visualization.group.levels !== undefined) {
    xSortable = {};

    for (let i = 0; i < visualization.group.levels.length; i++) {
      const level = visualization.group.levels[i];
      xSortable[level] = i;
    }
  }

  return { groupBy, xSortable };
}

export interface PrepareAggregatedData {
  xLabel: string;
  xValue: string;
  values: Record<string, number>;
  rowIds: Record<string, string[]>;
  sortBy: number | string;
  secondAxis?: boolean;
  tickerFormat?: TickerFormat;
}
