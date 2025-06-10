import chroma from "chroma-js";
import colors from "tailwindcss/colors";
export const lightScale = chroma
  .scale([colors.orange[700], colors.gray[700], colors.blue[700]])
  .domain([0, 0.5, 1])
  .mode("lab");

export const darkScale = chroma
  .scale([colors.orange[500], colors.gray[200], colors.blue[500]])
  .domain([0, 0.5, 1])
  .mode("lab");
