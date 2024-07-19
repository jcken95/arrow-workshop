library(dplyr)
library(arrow)

seattle_parquet_dir <- "data/seattle_library_checkouts_parquet"

seattle_csv <- arrow::open_dataset(
  sources = "data/seattle-library-checkouts.csv",
  format = "csv",
  col_types = arrow::schema(ISBN = arrow::string())
)

seattle_csv |>
  arrow::write_dataset(path = seattle_parquet_dir,
                       format = "parquet")
file <- list.files(seattle_parquet_dir, recursive = TRUE, full.names = TRUE)

# roughly half size of csv
file.size(file) / 10^9


arrow::open_dataset(seattle_parquet_dir) |>
  dplyr::filter(endsWith(MaterialType, "BOOK")) |>
  dplyr::group_by(CheckoutYear) |>
  dplyr::summarise(
    checkouts = sum(Checkouts)
  ) |>
  dplyr::arrange(CheckoutYear) |>
  dplyr::collect() |>
  system.time()
# large time saving over csv for same query

# partitioning data

seattle_parquet_part <- "data/seattle_library_partitioned"

seattle_csv |>
  dplyr::group_by(CheckoutYear) |>
  arrow::write_dataset(seattle_parquet_part, format = "parquet")

list.dirs(seattle_parquet_part, recursive = TRUE)


sizes <- tibble::tibble(
  files = list.files(seattle_parquet_part, recursive = TRUE),
  size_gb = round(
    file.size(file.path(seattle_parquet_part, files)) / 10^9,
    3
  )
)
sizes
# files relatively small :-)

arrow::open_dataset(seattle_parquet_part) |>
  dplyr::filter(endsWith(MaterialType, "BOOK")) |>
  dplyr::group_by(CheckoutYear) |>
  dplyr::summarise(
    checkouts = sum(Checkouts)
  ) |>
  dplyr::arrange(CheckoutYear) |>
  dplyr::collect() |>
  system.time()


arrow::open_dataset(seattle_parquet_dir) |>
  dplyr::filter(endsWith(MaterialType, "BOOK")) |>
  dplyr::group_by(CheckoutYear) |>
  dplyr::summarise(
    checkouts = sum(Checkouts)
  ) |>
  dplyr::arrange(CheckoutYear) |>
  dplyr::collect() |>
  system.time()


# filtering

open_dataset(sources = "/data/seattle-library-checkouts.csv",
             col_types = schema(ISBN = string()),
             format = "csv") |>
  filter(CheckoutYear == 2021, endsWith(MaterialType, "BOOK")) |>
  group_by(CheckoutMonth) |>
  summarize(TotalCheckouts = sum(Checkouts)) |>
  arrange(desc(CheckoutMonth)) |>
  collect() |>
  system.time()


open_dataset("data/seattle_library_checkouts_parquet",
             format = "parquet") |>
  filter(CheckoutYear == 2021, endsWith(MaterialType, "BOOK")) |>
  group_by(CheckoutMonth) |>
  summarize(TotalCheckouts = sum(Checkouts)) |>
  arrange(desc(CheckoutMonth)) |>
  collect() |>
  system.time()

# have partitioned data by checkout year - also filtered by checkout year
# this improves compute