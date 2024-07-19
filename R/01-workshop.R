library(arrow)
library(dplyr)

# data size (GB)
file.size("data/seattle-library-checkouts.csv") / 10^9


seattle_csv <- arrow::open_dataset(sources = "data/seattle-library-checkouts.csv",
                                   format = "csv")
seattle_csv
# ISBN null - type inference as failed us here
# first ~1mb data corresponds to empty ISBN col
nrow(seattle_csv)

# force ISBN string type
seattle_csv <- arrow::open_dataset(
  sources = "data/seattle-library-checkouts.csv",
  format = "csv",
  col_types = arrow::schema(ISBN = arrow::string())
)

arrow::schema(seattle_csv)
# ISBN now a string

