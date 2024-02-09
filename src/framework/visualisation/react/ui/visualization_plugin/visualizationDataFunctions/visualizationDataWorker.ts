import {
  ChartVisualization,
  TextVisualization,
  VisualizationType,
  VisualizationData,
  zChartVisualizationData,
  zTextVisualizationData,
  Table,
} from "../types";
import { prepareChartData } from "./prepareChartData";
import { prepareTextData } from "./prepareTextData";

interface Input {
  table: Table;
  visualization: VisualizationType;
}

self.onmessage = (e: MessageEvent<Input>) => {
  createVisualizationData(e.data.table, e.data.visualization)
    .then((visualizationData) => {
      self.postMessage({ status: "success", visualizationData });
    })
    .catch((error) => {
      console.error(error);
      self.postMessage({ status: "error", visualizationData: undefined });
    });
};

async function createVisualizationData(table: Table, visualization: VisualizationType): Promise<VisualizationData> {
  if (table === undefined || visualization === undefined) throw new Error("Table and visualization are required");

  try {
    if (["line", "bar", "area"].includes(visualization.type)) {
      const data = await prepareChartData(table, visualization as ChartVisualization);
      return zChartVisualizationData.parse(data);
    }

    if (["wordcloud"].includes(visualization.type)) {
      const data = await prepareTextData(table, visualization as TextVisualization);
      return zTextVisualizationData.parse(data);
    }
  } catch (e) {
    throw new Error(`Error creating visualization data: ${e}`);
  }

  throw new Error(`Visualization type ${visualization.type} not supported`);
}
