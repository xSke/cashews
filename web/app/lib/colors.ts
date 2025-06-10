import chroma from "chroma-js";
import colors from "tailwindcss/colors";

export const lightScale = chroma
  .scale([colors.blue[500], colors.neutral[600], colors.red[500]])
  .domain([0, 0.5, 1])
  .mode("lab");

export const darkScale = chroma
  .scale([colors.blue[500], colors.neutral[200], colors.red[500]])
  .domain([0, 0.5, 1])
  .mode("lab");
