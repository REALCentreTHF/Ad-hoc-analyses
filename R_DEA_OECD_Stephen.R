# integrated_read_health_data.R
if (!require("rsdmx")) install.packages("rsdmx")
if (!require("dplyr")) install.packages("dplyr")
if (!require("tidyr")) install.packages("tidyr")
if (!require("zoo")) install.packages("zoo")
if (!require("ggplot2")) install.packages("ggplot2")



library(rsdmx)
library(dplyr)
library(tidyr)
library(zoo)
library(ggplot2)

#countries <- c("AUS", "AUT", "BEL", "CAN", "CHL", "COL", "CRI", "CZE", "DNK", "EST", 
#               "FIN", "FRA", "DEU", "GRC", "HUN", "ISL", "IRL", "ISR", "ITA", "JPN",
#              "KOR", "LVA", "LTU", "LUX", "MEX", "NLD", "NZL", "NOR", "POL", "PRT",
#             "SVK", "SVN", "ESP", "SWE", "CHE", "TUR", "GBR")

countries <- c("AUS", "AUT", "BEL", "CAN", "CHE", "DEU", "DNK", "ESP", "FIN",
                    "FRA", "GBR", "IRL", "ITA", "NLD", "NOR", "NZL", "SWE", "USA")



# ---- STEP 1: LOAD DATA ----
# Load the .rds file
setwd("C:/Users/Yi.Mu/OneDrive - The Health Foundation/Documents/productivity commision/DEA OECD")
raw_data <- readRDS("all_raw_data.rds")

# ---- STEP 2: PROCESS EACH DATASET MANUALLY ----

# 1️⃣ HEALTH SPENDING
message("🔍 Processing health_spending")
df <- raw_data$health_spending
print(str(df))
print(head(df))

# Check dimensions before filtering
print("Unique MEASURE values:")
print(unique(df$MEASURE))

# Filter to PPP per person
df_clean <- df %>%
  filter(UNIT_MEASURE == "USD_PPP_PS") %>%
  select(country = REF_AREA, year = obsTime, value = obsValue, everything()) %>%
  mutate(year = as.integer(year), value = as.numeric(value)) %>%
  filter(country %in% countries)

print("Check cleaned spending data:")
print(head(df_clean))

readline(prompt = "✅ Check health_spending cleaned structure. Press Enter to impute...")

df_clean <- df_clean %>%
  group_by(country) %>%
  complete(year = seq(min(year, na.rm = TRUE), max(year, na.rm = TRUE))) %>%
  arrange(year) %>%
  mutate(value_filled =zoo::na.approx(value, x = year, maxgap = 7, rule = 2, na.rm = FALSE)) %>%
  ungroup()



# Suppose this is your dynamic column name
name <- "health_spending"

df_clean <- df_clean %>%
  select(country, year, !!name := value_filled)


saveRDS(df_clean, "health_spending_processed.rds")
write.csv(df_clean, "health_spending_processed.csv", row.names = FALSE)

# 2️⃣ RISK FACTORS
message("🔍 Processing risk_factors")
df <- raw_data$risk_factors
print(str(df))
print(head(df))

print("Unique SOURCE values (self-report vs measured):")
print(unique(df$METHODOLOGY))
print(unique(df$MEASURE))

# Prioritise measured, fallback to self-report
df_clean <- df %>%
  select(country = REF_AREA, year = obsTime, value = obsValue, METHODOLOGY, MEASURE, everything()) %>%
  mutate(year = as.integer(year), value = as.numeric(value)) %>%
  filter(country %in% countries) %>%
  group_by(country, MEASURE, year) %>%
  arrange(desc(grepl("OBS", METHODOLOGY))) %>%
  slice(1) %>%
  ungroup()

print("Check risk_factors after prioritising measured:")
print(head(df_clean))

readline(prompt = "✅ Check risk_factors cleaned structure. Press Enter to impute...")

df_clean <- df_clean %>%
  group_by(country, MEASURE) %>%
  complete(year = seq(min(year, na.rm = TRUE), max(year, na.rm = TRUE))) %>%
  arrange(year) %>%
  mutate(value_filled = zoo::na.approx(value, x = year, maxgap = 7, rule = 2, na.rm = FALSE)) %>%
  ungroup() %>% 
  select(country, year, MEASURE , value_filled) %>%
  pivot_wider(names_from = MEASURE, values_from = value_filled) 

saveRDS(df_clean, "risk_factors_processed.rds")
write.csv(df_clean, "risk_factors_processed.csv", row.names = FALSE)

# 3️⃣ MORTALITY
message("🔍 Processing mortality_all")
df <- raw_data$mortality_all
print(str(df))
print(head(df))

print("Unique CAUSE values:")
print(unique(df$CAUSE))

df_clean <- df %>%
  select(country = REF_AREA, year = obsTime, value = obsValue, everything()) %>%
  mutate(year = as.integer(year), value = as.numeric(value)) %>%
  filter(country %in% countries)

readline(prompt = "✅ Check mortality_all structure. Press Enter to impute...")

df_clean <- df_clean %>%
  group_by(country, MEASURE) %>%
  complete(year = seq(min(year, na.rm = TRUE), max(year, na.rm = TRUE))) %>%
  arrange(year) %>%
  mutate(value_filled = zoo::na.approx(value, x = year, maxgap = 7, rule = 2, na.rm = FALSE)) %>%
  ungroup() %>% 
  select(country, year, MEASURE , value_filled) %>%
  pivot_wider(names_from = MEASURE, values_from = value_filled) 

saveRDS(df_clean, "mortality_all_processed.rds")
write.csv(df_clean, "mortality_all_processed.csv", row.names = FALSE)

# Repeat similar steps for acsa, nurses, doctors, hospital_beds
# Each block will:
# - Print str + head
# - Show unique dimensions (if any)
# - Apply basic cleaning (country, year, value)
# - Pause for manual review
# - Impute missing
# - Save processed output

# Example for acsa:
message("🔍 Processing acsa")
df <- raw_data$acsa
print(str(df))
print(head(df))

df_clean <- df %>%
  select(country = REF_AREA, year = obsTime, value = obsValue, everything()) %>%
  mutate(year = as.integer(year), value = as.numeric(value)) %>%
  filter(country %in% countries)

readline(prompt = "✅ Check acsa structure. Press Enter to impute...")

df_clean <- df_clean %>%
  group_by(country, MEASURE) %>%
  complete(year = seq(min(year, na.rm = TRUE), max(year, na.rm = TRUE))) %>%
  arrange(year) %>%
  mutate(value_filled = zoo::na.approx(value, x = year, maxgap = 7, rule = 2, na.rm = FALSE)) %>%
  ungroup() %>% 
  select(country, year, MEASURE , value_filled) %>%
  pivot_wider(names_from = MEASURE, values_from = value_filled) 

df_clean <- df_clean %>%  
  filter(if_any(3:5, ~ !is.na(.))) %>% 
  select(-`NA`)

saveRDS(df_clean, "acsa_processed.rds")
write.csv(df_clean, "acsa_processed.csv", row.names = FALSE)

####

# 4️⃣ NURSES
message("🔍 Processing nurses")
df <- raw_data$nurses
print(str(df))
print(head(df))

df_clean <- df %>%
  select(country = REF_AREA, year = obsTime, value = obsValue, everything()) %>%
  mutate(year = as.integer(year), value = as.numeric(value)) %>%
  filter(country %in% countries)

print("Unique combinations of dimensions in nurses:")
print(df_clean %>% select(-country, -year, -value) %>% distinct())

readline(prompt = "✅ Check nurses structure. Press Enter to impute...")

df_clean <- df_clean %>%
  group_by(country) %>%
  complete(year = seq(min(year, na.rm = TRUE), max(year, na.rm = TRUE))) %>%
  arrange(year) %>%
  mutate(value_filled = na.approx(value, x = year, maxgap = 7, rule = 2, na.rm = FALSE)) %>%
  ungroup()

saveRDS(df_clean, "nurses_processed.rds")
write.csv(df_clean, "nurses_processed.csv", row.names = FALSE)

# 5️⃣ DOCTORS
message("🔍 Processing doctors")
df <- raw_data$doctors
print(str(df))
print(head(df))

df_clean <- df %>%
  select(country = REF_AREA, year = obsTime, value = obsValue, everything()) %>%
  mutate(year = as.integer(year), value = as.numeric(value)) %>%
  filter(country %in% countries)

print("Unique combinations of dimensions in doctors:")
print(df_clean %>% select(-country, -year, -value) %>% distinct())

readline(prompt = "✅ Check doctors structure. Press Enter to impute...")

df_clean <- df_clean %>%
  group_by(country) %>%
  complete(year = seq(min(year, na.rm = TRUE), max(year, na.rm = TRUE))) %>%
  arrange(year) %>%
  mutate(value_filled = na.approx(value, x = year, maxgap = 7, rule = 2, na.rm = FALSE)) %>%
  ungroup()

saveRDS(df_clean, "doctors_processed.rds")
write.csv(df_clean, "doctors_processed.csv", row.names = FALSE)

# 6️⃣ HOSPITAL BEDS
message("🔍 Processing hospital_beds")
df <- raw_data$hospital_beds
print(str(df))
print(head(df))

df_clean <- df %>%
  select(country = REF_AREA, year = obsTime, value = obsValue, everything()) %>%
  mutate(year = as.integer(year), value = as.numeric(value)) %>%
  filter(country %in% countries)

print("Unique combinations of dimensions in hospital_beds:")
print(df_clean %>% select(-country, -year, -value) %>% distinct())

readline(prompt = "✅ Check hospital_beds structure. Press Enter to impute...")

df_clean <- df_clean %>%
  group_by(country) %>%
  complete(year = seq(min(year, na.rm = TRUE), max(year, na.rm = TRUE))) %>%
  arrange(year) %>%
  mutate(value_filled = na.approx(value, x = year, maxgap = 7, rule = 2, na.rm = FALSE)) %>%
  ungroup()

saveRDS(df_clean, "hospital_beds_processed.rds")
write.csv(df_clean, "hospital_beds_processed.csv", row.names = FALSE)

# 7️⃣ ACSA (already partially written above, included again for completeness)
message("🔍 Processing acsa")
df <- raw_data$acsa
print(str(df))
print(head(df))

df_clean <- df %>%
  select(country = REF_AREA, year = obsTime, value = obsValue, everything()) %>%
  mutate(year = as.integer(year), value = as.numeric(value)) %>%
  filter(country %in% countries)

print("Unique combinations of dimensions in acsa:")
print(df_clean %>% select(-country, -year, -value) %>% distinct())

readline(prompt = "✅ Check acsa structure. Press Enter to impute...")

df_clean <- df_clean %>%
  group_by(country) %>%
  complete(year = seq(min(year, na.rm = TRUE), max(year, na.rm = TRUE))) %>%
  arrange(year) %>%
  mutate(value_filled = na.approx(value, x = year, maxgap = 7, rule = 2, na.rm = FALSE)) %>%
  ungroup()

saveRDS(df_clean, "acsa_processed.rds")
write.csv(df_clean, "acsa_processed.csv", row.names = FALSE)


# 8 Hospital Discharges (already partially written above, included again for completeness)
message("🔍 Processing discharge")
df <- raw_data$discharges
print(str(df))
print(head(df))

df_clean <- df %>%
  select(country = REF_AREA, year = obsTime, value = obsValue, everything()) %>%
  mutate(year = as.integer(year), value = as.numeric(value)) %>%
  filter(country %in% countries)

print("Unique combinations of dimensions in discharges:")
print(df_clean %>% select(-country, -year, -value) %>% distinct())

readline(prompt = "✅ Check dischages structure. Press Enter to impute...")

df_clean <- df_clean %>%
  group_by(country) %>%
  complete(year = seq(min(year, na.rm = TRUE), max(year, na.rm = TRUE))) %>%
  arrange(year) %>%
  mutate(value_filled = na.approx(value, x = year, maxgap = 7, rule = 2, na.rm = FALSE)) %>%
  ungroup()

saveRDS(df_clean, "discharges_processed.rds")
write.csv(df_clean, "discharges_processed.csv", row.names = FALSE)


# 6️⃣ GDP PER CAPITA
message("🔍 Processing GDP_per_capita")
df <- raw_data$GDP_per_capita
print(str(df))
print(head(df))

df_clean <- df %>%
  select(country = REF_AREA, year = obsTime, value = obsValue, everything()) %>%
  mutate(year = as.integer(year), value = as.numeric(value)) %>%
  filter(country %in% countries)

print("Unique combinations of dimensions in GDP_per_capita:")
print(df_clean %>% select(-country, -year, -value) %>% distinct())

readline(prompt = "✅ Check GDP_per_capita structure. Press Enter to impute...")

df_clean <- df_clean %>%
  group_by(country) %>%
  complete(year = seq(min(year, na.rm = TRUE), max(year, na.rm = TRUE))) %>%
  arrange(year) %>%
  mutate(value_filled = na.approx(value, x = year, maxgap = 7, rule = 2, na.rm = FALSE)) %>%
  ungroup()

saveRDS(df_clean, "GDP_per_capita_processed.rds")
write.csv(df_clean, "GDP_per_capita_processed.csv", row.names = FALSE)

message("✅ All datasets processed manually.")


#####

# Load cleaned datasets
health_spending <- readRDS("health_spending_processed.rds")
mortality_all <- readRDS("mortality_all_processed.rds")
risk_factors <- readRDS("risk_factors_processed.rds")
acsa <- readRDS("acsa_processed.rds")
nurses <- readRDS("nurses_processed.rds")
doctors <- readRDS("doctors_processed.rds")
hospital_beds <- readRDS("hospital_beds_processed.rds")
discharges <- readRDS("discharges_processed.rds")
GDP_per_capita <- readRDS("GDP_per_capita_processed.rds")


# Inspect structure (optional)
message("✅ Health spending:")
print(str(health_spending))
message("✅ Mortality:")
print(str(mortality_all))
message("✅ Risk factors:")
print(str(risk_factors))
message("✅ Avoidable admissions:")
print(str(acsa))
message("✅ Nurses:")
print(str(nurses))
message("✅ Doctors:")
print(str(doctors))
message("✅ Hospital beds:")
print(str(hospital_beds))
message("✅  GDP per capita:")
print(str(GDP_per_capita))

message("✅ Hospital discharges:")
print(str(discharges))

# Merge all datasets
panel_data <- health_spending %>%
  full_join(mortality_all, by = c("country", "year")) %>%
  full_join(risk_factors, by = c("country", "year")) %>%
  full_join(acsa, by = c("country", "year")) %>%
  full_join(nurses, by = c("country", "year")) %>%
  full_join(doctors, by = c("country", "year")) %>%
  full_join(hospital_beds, by = c("country", "year")) %>%
  full_join(GDP_per_capita, by = c("country", "year")) 
  
  
# full_join(discharges, by = c("country", "year")) ##### NEED TO CHECK!!!


  

# Final check
message("✅ Combined panel data structure:")
print(str(panel_data))
print(head(panel_data))

# Save the combined panel
saveRDS(panel_data, "combined_health_panel.rds")
write.csv(panel_data, "combined_health_panel.csv", row.names = FALSE)

message("✅ Combined panel saved.")


###

if (!require("Benchmarking")) install.packages("Benchmarking")
library(Benchmarking)
library(dplyr)

# Load combined panel
panel_data <- readRDS("combined_health_panel.rds")


# Create period label (5 years per group)
panel_data <- panel_data %>%
  mutate(Period = case_when(
    year %in% 2000:2004 ~ "2000-2004",
    year %in% 2005:2009 ~ "2005-2009",
    year %in% 2010:2014 ~ "2010-2014",
    year %in% 2015:2019 ~ "2015-2019",
    year %in% 2020:2024 ~ "2020-2024",
    TRUE ~ NA_character_
  )) %>%
  filter(!is.na(Period))


# Create period label (4 years per group)
panel_data <- panel_data %>%
  mutate(Period = case_when(
    year %in% 2001:2004 ~ "2001-2004",
    year %in% 2005:2008 ~ "2005-2008",
    year %in% 2009:2012 ~ "2009-2012",
    year %in% 2013:2016 ~ "2013-2016",
    year %in% 2017:2020 ~ "2017-2020",
    year %in% 2021:2024 ~ "2021-2024",
    TRUE ~ NA_character_
  )) %>%
  filter(!is.na(Period))


# Create period label (3 or 4 years per group)
panel_data <- panel_data %>%
  mutate(Period = case_when(
    year %in% 2001:2004 ~ "2001-2004",
    year %in% 2005:2007 ~ "2005-2007",
    year %in% 2008:2010 ~ "2008-2010",
    year %in% 2011:2013 ~ "2011-2013",
    year %in% 2014:2016 ~ "2014-2016",
    year %in% 2017:2020 ~ "2017-2020",
    year %in% 2021:2024 ~ "2021-2024",
    TRUE ~ NA_character_
  )) %>%
  filter(!is.na(Period))


# Compute period averages
panel_avg <- panel_data %>%
  group_by(country, Period) %>%
  summarise(
    health_spending = mean(health_spending, na.rm = TRUE),
    treat_mort = mean(TRTM, na.rm = TRUE),  # Adjust if you have TRTM column separately
    smoking = mean(SP_DS, na.rm = TRUE),
    overweight_obese = mean(SP_OVRGHT_OBS, na.rm = TRUE),
    .groups = "drop"
  )

panel_avg <- panel_avg %>%
  mutate(treat_mort_inv = max(treat_mort, na.rm = TRUE) + 1 - treat_mort)


# Run DEA
# Inputs: health_spending, SP_DS
# Output: TRTM
dea_results_list <- list()

for (p in unique(panel_avg$Period)) {
  df_period <- panel_avg %>% filter(Period == p) %>%
    filter(!is.na(health_spending) & !is.na(treat_mort) & !is.na(smoking) & !is.na(overweight_obese))
  
  if (nrow(df_period) > 0) {
    dea_model <- dea(
      X = as.matrix(df_period %>% select(health_spending)),
      Y = as.matrix(df_period %>% select(treat_mort_inv)),
      RTS = "vrs",
      ORIENTATION = "in"  # Correct orientation keyword
    )
    
    df_period$DEA_efficiency <- dea_model$eff
    dea_results_list[[p]] <- df_period
  }
}

# Combine results
dea_results <- bind_rows(dea_results_list)

# Save
saveRDS(dea_results, "dea_results_by_period.rds")
write.csv(dea_results, "dea_results_by_period.csv", row.names = FALSE)

# Plot using reorder_within
if (!require("tidytext")) install.packages("tidytext")
library(tidytext)

ordering <- dea_results %>%
  group_by(Period) %>%
  arrange(desc(DEA_efficiency)) %>%
  mutate(order_rank = row_number()) %>%
  select(country, Period, order_rank)

# Merge this ordering into dea_results
dea_results <- dea_results %>%
  left_join(ordering, by = c("country", "Period")) %>%
  mutate(country_ordered = reorder_within(country, -order_rank, Period))

dea_results <- dea_results %>%
  mutate(UK_highlight = ifelse(country == "GBR", "UK", "Other")) %>%
  mutate(country_ordered = reorder_within(country, DEA_efficiency, Period))


# Plot
ggplot(dea_results, aes(x = country_ordered, y = DEA_efficiency, fill = UK_highlight)) +
  geom_col() +
  scale_fill_manual(values = c("UK" = "red", "Other" = "grey70")) +
  facet_wrap(~ Period, scales = "free_x") +
  scale_x_reordered() +
  coord_flip() +
  ylab("DEA Efficiency Score") +
  xlab("Country") +
  ggtitle("DEA Efficiency Scores by Country and Period (UK highlighted)") +
  theme_minimal()

# Save plot
ggsave("dea_efficiency_score.png", width = 10, height = 6)

# Regress efficiency on smoking + obesity
adjusted_list <- list()

for (p in unique(dea_results$Period)) {
  df_p <- dea_results %>% filter(Period == p)
  
  # Check we have data
  if (nrow(df_p) > 0) {
    adjust_model <- lm(DEA_efficiency ~ smoking + overweight_obese, data = df_p)
    
    df_p <- df_p %>%
      mutate(
        DEA_efficiency_adj = residuals(adjust_model),
        DEA_efficiency_adj_scaled = DEA_efficiency_adj - min(DEA_efficiency_adj, na.rm = TRUE) + 0.01
      )
    
    adjusted_list[[p]] <- df_p
  }
}

# Combine all periods
adjusted_results <- bind_rows(adjusted_list)

# Get adjusted efficiency = residuals
plot_df <- adjusted_results %>%
  select(country, Period, Unadjusted = DEA_efficiency, Adjusted = DEA_efficiency_adj_scaled) %>%
  pivot_longer(cols = c(Unadjusted, Adjusted), names_to = "ScoreType", values_to = "Efficiency")

ggplot(plot_df, aes(x = reorder_within(country, Efficiency, Period), y = Efficiency, fill = ScoreType)) +
  geom_col(position = position_dodge()) +
  facet_wrap(~ Period, scales = "free_x") +
  scale_x_reordered() +
  coord_flip() +
  scale_fill_manual(values = c("Unadjusted" = "grey70", "Adjusted" = "red")) +
  ylab("Efficiency Score") +
  xlab("Country") +
  ggtitle("DEA Efficiency Scores: Adjusted vs Unadjusted by Period") +
  theme_minimal()

# Save plot
ggsave("dea_efficiency_adjusted_vs_unadjusted.png", width = 10, height = 6)

message("✅ DEA analysis complete, results saved, plot created.")
