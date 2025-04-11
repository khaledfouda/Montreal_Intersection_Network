setwd("~/OneDrive/coursework/Winter25/networks/project/Montreal_Intersection_Network/src")
source("./load_libs.R")

d <- readRDS("../data/processed_data_1033.rds")
montreal <- readRDS("../data/montreal_processed.rds")
d$dfs <- d$df %>%
  as.data.frame() %>%
  select(tot_crossw, acc, node_id,
         total_lane, number_of_, of_exclusi, any_exclus, #intersection size
         fi, pi, # traffic flow
         parking, median, # road design
         all_pedest, ped_countd, half_phase, lt_protect, #pedest. protection
         distdt, borough,  #neighborhood
         commercial)
skim(d$dfs)

d$nodes %<>%
  select(deg_unweighted, betweenes, harmonic, eccentricity, geometry, node.id) %>%
  mutate(eccentricity = 1 / eccentricity)

# corr central
cor_m <- cor(d$nodes %>% as.data.frame() %>% select(-geometry, -node.id),
             method = "spearman")
# cor_m[abs(cor_m)<.3] = NA
ggcorrplot::ggcorrplot(cor_m,lab = T,type = "upper", insig="blank",
                       title = "Correlation Coefficients (>.3) between Centrality Measures",
                       show.diag = F, show.legend = F)
# corr cov:



cor_m <- cor(d$dfs  %>% select( -node_id, -borough))
cor_m[abs(cor_m)<.3] = NA
ggcorrplot::ggcorrplot(cor_m,lab = T,type = "upper", insig="blank",
                       title = "Correlation Coefficients (>.3) between features",
                       show.diag = F, show.legend = F)

#---------------------------------------
ggplot() +
  geom_sf(data = montreal, color = "grey80", size = 0.3) +
  geom_sf(data = d$nodes, 
          aes(color = harmonic), 
          size = 1) +
  #scale_color_discrete() +  
  scale_colour_viridis_b()+
  labs(color = "Harmonic",
       title = "Intersections colored by Weighted Harmonic Centrality") +
  theme_bw() +
  theme(panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        axis.title       = element_blank(),
        axis.text        = element_blank(),
        axis.ticks       = element_blank(),
        axis.line        = element_blank())

hist(d$nodes$harmonic)
#--------------
ggplot() +
  geom_sf(data = montreal, color = "grey80", size = 0.3) +
  geom_sf(data = d$nodes, 
          aes(color = betweenes), 
          size = 1) +
  #scale_color_discrete() + 
  scale_colour_viridis_c()+
  labs(color = "Betweeness",
       title = "Intersections colored by Weighted Betweeness Centrality") +
  theme_bw() +
  theme(panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        axis.title       = element_blank(),
        axis.text        = element_blank(),
        axis.ticks       = element_blank(),
        axis.line        = element_blank())
hist(d$nodes$betweenes)
#---------------------------------------
ggplot() +
  geom_sf(data = montreal, color = "grey80", size = 0.3) +
  geom_sf(data = d$nodes, 
          aes(color = eccentricity), 
          size = 1) +
  scale_colour_viridis_c()+
  #scale_color_discrete() +  
  labs(color = "Cluster",
       title = "Intersections colored by Assigned Clusters") +
  theme_minimal()
hist(d$nodes$eccentricity)


#----------
# prepare final dataset
d$dfs %<>%
  select( - of_exclusi) %>%
  rename(node.id = node_id) %>%
  left_join(
    d$nodes %>%
      as.data.frame() %>%
      select(-geometry) %>%
      #select(node.id, deg_unweighted,harmonic , betweenes, closeness, eigcentrality) %>%
      rename_with(~ paste0("centr_",.)) %>% 
      rename(node.id = centr_node.id),
    "node.id"
  ) %>% 
  arrange(node.id) 
#--------------------------------------------------------------------
# overall correlation
cor_m <- cor(d$dfs  %>% select( -node.id, -borough))
cor_m[abs(cor_m)<.4] = NA
ggcorrplot::ggcorrplot(cor_m,lab = T,type = "upper", insig="blank",
                       title = "Correlation Coefficients (>.4) between Centralities and features",
                       show.diag = F, show.legend = F)
#------------------------------------------------
ggplot(d$dfs, aes(x = distdt, y = centr_harmonic)) +
      geom_point(alpha = 0.6, size = 1.2, colour = "steelblue") +
      geom_smooth(method = "loess", se = FALSE, colour = "darkred", linewidth = 1) +
     scale_x_continuous(labels = label_comma(), name = "Distance from downtown (m)") +
      scale_y_continuous(labels = label_scientific(digits = 2),
                                         name  = "Harmonic centrality") +
      labs(title = "Harmonic centrality vs. distance from downtown",
                   subtitle = "Each point = one intersection") +
       theme_bw()
#-----------------------------------------------------------------------
# clustering nodes - with features + centralities
d$dfs %>%
  mutate(across(where(is.numeric),
                ~ if(n_distinct(.x)==2 || cur_column()=="node.id") 
                  .x else as.numeric(scale(.x)))) ->
  d$dfs.scaled
skim(d$dfs.scaled)

set.seed(2025); d$dfs.scaled %>%
  select(-node.id, -borough) %>% 
  kmeans(4,nstart = 20) ->
  kmfit
kmfit$size
d$nodes$clusterA <- as.factor(kmfit$cluster)


ggplot() +
  geom_sf(data = montreal, color = "grey80", size = 0.3) +
  geom_sf(data = d$nodes, 
          aes(color = clusterA), 
          size = 1) +
  #scale_color_discrete() +  
  labs(color = "Cluster",
       title = "Intersections colored by Assigned Clusters") +
  theme_minimal()

# the following part is only to confirm that the clusters are good. 
set.seed(2025); d$dfs.scaled %>%
  select(-node.id, -borough) %>% #select(contains("centr_")) %>%
  prcomp() -> prf
data.frame(prf$x[,1:2], cluster=as.factor(kmfit$cluster)) %>%
  ggplot(aes(PC1, PC2, color=cluster)) +
  geom_point(alpha=0.7)
#------------------------------------------------------------
# clustering nodes -centralities only

set.seed(2025); d$dfs.scaled %>%
  select(-node.id, -borough) %>% select(contains("centr_")) %>%
  kmeans(3,nstart = 20) ->
  kmfit
kmfit$size
d$nodes$clusterB <- as.factor(kmfit$cluster)


ggplot() +
  geom_sf(data = montreal, color = "grey80", size = 0.3) +
  geom_sf(data = d$nodes, 
          aes(color = clusterB), 
          size = 1) +
  #scale_color_discrete() +  
  labs(color = "Cluster",
       title = "Intersections colored by Assigned Clusters",
       subtitle = "based on centralities") +
  theme_bw() +
  theme(panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        axis.title       = element_blank(),
        axis.text        = element_blank(),
        axis.ticks       = element_blank(),
        axis.line        = element_blank()) 

# the following part is only to confirm that the clusters are good. 
set.seed(2025); d$dfs.scaled %>%
  select(-node.id, -borough) %>% select(contains("centr_")) %>%
  prcomp() -> prf
data.frame(prf$x[,1:2], cluster=as.factor(kmfit$cluster)) %>%
  ggplot(aes(PC1, PC2, color=cluster)) +
  geom_point(alpha=0.7)
#-------------------------------------------------------------
d$dfs %>%
  as.data.frame() %>%
  mutate(cluster = d$nodes$clusterB) %>%
  group_by(cluster) %>%
  summarise(                                   
    across(
      .cols   = where(is.numeric),            
      .fns    = ~round(mean(.x, na.rm = TRUE),2)#list(
      #   mean = ~round(mean(.x, na.rm = TRUE),2)
      #  # sd   = ~sd(.x,   na.rm = TRUE)
      # ),
      # .names = "{.col}_{.fn}"                  
    )
  ) %>% 
  t() #%>%
  mutate(across("all",~as.numeric(.)))
  #ungroup() 

  library(ggalt)
  cluster_means <- d$dfs %>%
    as.data.frame() %>%
    mutate(cluster = d$nodes$clusterB) %>% 
    group_by(cluster) %>% 
    summarise(acc_mean = mean(acc, na.rm=TRUE),
              fi_mean  = mean(fi , na.rm=TRUE))
  
  ggplot(cluster_means,
         aes(x = fi_mean, xend = acc_mean, y = factor(cluster))) +
    geom_dumbbell(size = 3, colour = "#2c3e50",
                  colour_x = "#3498db", colour_xend = "#e74c3c") +
    scale_x_continuous(labels = comma,
                       name   = "Vehicle flow (fi)   ←                 →   Accident count (acc)") +
    labs(title = "Clusters: traffic exposure vs. crash outcome") +
    theme_minimal()



  library(ggplot2)
  
  d$dfs %>%
    as.data.frame() %>%
    mutate(cluster = d$nodes$clusterB) %>%
  ggplot(aes(x = distdt, y = centr_harmonic, colour = factor(cluster))) +
    geom_point(alpha = 0.6) +
    geom_smooth(method = "lm", se = FALSE, linewidth = 1) +
    scale_colour_brewer(palette = "Set1", name = "Cluster") +
    scale_x_continuous(labels = comma, name = "Distance to downtown (m)") +
    scale_y_continuous(labels = scientific, name = "Harmonic centrality") +
    labs(title = "Accessibility declines with distance; clusters separate clearly") +
    theme_minimal()

  vars <- c("acc", "fi", "pi",
            "centr_betweenes", "centr_harmonic", "distdt",
            "median", "commercial")
  
  # --- compute the cluster‑wise means ------------------------------
  cluster_tbl <- 
    d$dfs %>%
    as.data.frame() %>%
    mutate(cluster = d$nodes$clusterB) %>%
                               # <- replace with your data frame
    group_by(cluster) %>% 
    summarise(across(all_of(vars),
                     ~ round(sum(.x, na.rm = TRUE), 2))) %>% 
    # reorder columns: cluster first, then everything else
    relocate(cluster)
  # nn <- ncol(cluster_tbl)
  # cluster_tbl[1,2:nn] <- (cluster_tbl[1,2:nn] / cluster_tbl[2,2:nn] * 100) %>% round(2)
  # cluster_tbl[3,2:nn] <- (cluster_tbl[3,2:nn] / cluster_tbl[2,2:nn] * 100) %>% round(2)
  # --- print the table ---------------------------------------------
  kable(cluster_tbl,
        caption = "Key means by cluster",
        align   = "c") %>%
    kable_styling(full_width = FALSE, position = "center")

  
  
  cluster_stats <-d$dfs %>%
    as.data.frame() %>%
    mutate(cluster = d$nodes$clusterB) %>%                      # <- replace with your data frame
    group_by(cluster) %>% 
    summarise(
      acc_mean     = round(mean(acc, na.rm = TRUE),2),
      acc_median     = median(acc, na.rm = TRUE),                 # median crashes
      fi_median_k    = round(median(fi, na.rm = TRUE) / 1000, 2), #
      pi_median_k    = round(median(pi, na.rm = TRUE) / 1000, 2), # )
      median_prop    = round(mean(median == 1, na.rm = TRUE), 2)  # ian
    ) %>% 
    rename(`Accidents (mean)`            = acc_mean,
            `Accidents (median)`            = acc_median,
           `Vehicle flow ×1k (median)`     = fi_median_k,
           `Pedestrian flow ×1k (median)`  = pi_median_k,
           `Road‑median present (proportion)` = median_prop) %>%
    mutate(Size = kmfit$size)
  
  # ---- nicely print the table ------------------------------------------
  kable(cluster_stats,
        caption = "Cluster summaries",
        align   = "c") %>% 
    kable_styling(full_width = FALSE, position = "center")
  

#-----------------------------------------------------------------
# modelling: -- using the scaled dataset with clusters: --

d$dfs.scaled %<>%
  mutate(clusterA = as.factor(d$nodes$clusterA),
         clusterB = as.factor(d$nodes$clusterB),
         borough = as.factor(borough),
         acc = d$dfs$acc)
# 0 inflated :(
hist(d$dfs.scaled$acc)
# mean != var
mean(d$dfs.scaled$acc)
var(d$dfs.scaled$acc)
# 1. poisson model:
d$dfs.scaled %>%
  select( - node.id, -borough, ) %>%
  glm(formula= acc ~ ., family="poisson") -> poisfit
summary(poisfit)
# dispersion test
AER::dispersiontest(poisfit) 

d$dfs.scaled %>%
  select( - node.id, -borough,-clusterA, -clusterB, -contains("centr_")) %>%
  MASS::glm.nb(formula=acc ~ .) %>% summary()

d$dfs.scaled %>%
  select( acc, contains("centr_"),) %>%
  MASS::glm.nb(formula=acc ~ .) %>% summary()

d$dfs.scaled %>%
  select(clusterA, clusterB, acc) %>%
  MASS::glm.nb(formula=acc ~ .) %>% summary()

d$dfs.scaled %>%
  select( - node.id, -borough,) %>%
  MASS::glm.nb(formula=acc ~ .) %>% summary()

d$dfs.scaled %>%
  select( - node.id, -borough, -contains("centr_")) %>%
  MASS::glm.nb(formula=acc ~ .) %>% summary()


# model with sig covariates only <unnormalized so we can interpret them>
d$dfs %>%
  mutate(clusterA = as.factor(d$nodes$clusterA)) %>%
  select(acc, all_pedest, fi, pi, half_phase, commercial,
         contains("centr_"), - centr_eccentricity) %>%
  MASS::glm.nb(formula=acc ~ .   ) -> nbfit
summary(nbfit) 

exp(coef(nbfit))

# model with sig covariates only <unnormalized so we can interpret them>
d$dfs %>%
  mutate(clusterA = as.factor(d$nodes$clusterA)) %>%
  select(acc, all_pedest, fi, pi, half_phase, commercial,
         clusterA) %>%
  MASS::glm.nb(formula=acc ~ .   ) -> nbfit2
summary(nbfit2) 
# interpret them>>
exp(coef(nbfit2))

# 
# d$dfs.scaled %>%
#   select(acc, all_pedest, fi, pi, half_phase, commercial,
#          clusterA) %>%
#   pscl::zeroinfl(formula = acc ~ . , dist="negbin") ->
#   zinfpoi
#   summary(zinfpoi)
# 
# pscl::vuong(nbfit, zinfpoi)


#interpretation >>>>>>>>>>>
broom::tidy(nbfit, exponentiate = TRUE) %>%
  mutate(
    pct_change = (estimate - 1) * 100,
    Effect = case_when(
      round(estimate,3) > 1 ~ paste0("+", round(pct_change, 1),"%"),
      round(estimate,3) < 1 ~ paste0(round(pct_change, 1),"%"),
      TRUE         ~ "No change"
    )
  ) %>%
  arrange(pct_change) %>%
  rename(
    Term        = term,
    `Exp(Coef)` = estimate,
    `Std.Error` = std.error,
    `Z-value`   = statistic,
    `P-value`   = p.value
  ) %>%
  mutate(`P-value` = round(`P-value`, 3)) %>%
  select(Term, `Exp(Coef)`, Effect, `P-value`) %>%
  kable()
#################################################################
library(kableExtra)
library(broom)
tab <- tidy(nbfit, exponentiate = TRUE) %>%      # nbfit = your neg‑bin model
  mutate(
    pct_change = (estimate - 1) * 100,
    Effect = case_when(
      round(estimate, 3) > 1  ~ paste0("+", round(pct_change, 1), "%"),
      round(estimate, 3) < 1  ~ paste0(round(pct_change, 1), "%"),
      TRUE                    ~ "No change"
    )
  )

# ---- 2.  replace technical names with slide‑friendly labels ----
label_lookup <- c(
  "(Intercept)"           = "Baseline (all covariates = 0)",
  "all_pedest"            = "All‑pedestrian phase (yes = 1)",
  "half_phase"            = "Semi‑protected ped phase (yes = 1)",
  "centr_betweenes"       = "Betweenness centrality",
  "fi"                    = "Vehicle flow (avg daily)",
  "pi"                    = "Pedestrian flow (avg daily)",
  "commercial"            = "Commercial driveways (count)",
  "centr_deg_unweighted"  = "Unweighted degree (#connections)",
  "centr_harmonic"        = "Harmonic centrality"
)

tab <- tab %>%
  mutate(Variable = label_lookup[term])          # map names → labels
tab[-1,] -> tab

# ---- 3.  final table for slides ----
present_tbl <- tab %>%
  arrange(desc(abs(pct_change))) %>%              # optional ordering
  transmute(
    Variable,
    `Exp(coef)`       = round(estimate, 3),
    `Effect`    = Effect
    #`P‑value`   = round(signif(p.value, 3),3)
  ) 

kable(present_tbl,
      caption = "Negative‑binomial model: #accidents vs covariates+centralities",
      align   = "lccc") %>% 
  kable_styling(full_width = FALSE, position = "center")
############################
# model fit

library(MASS)   # nbfit already built with glm.nb
library(pscl)   # for pR2 if you like

# --- 1. log‑likelihoods -------------------------------------------------
ll_mod  <- as.numeric(logLik(nbfit))          # fitted model
null_nb <- MASS::glm.nb(acc ~ 1, data = model.frame(nbfit))
ll_null <- as.numeric(logLik(null_nb))
ll_null <- as.numeric(logLik(update(nbfit, . ~ 1)))   # intercept‑only

n <- nobs(nbfit)

# --- 2. pseudo‑R² -------------------------------------------------------
R2_mcfadden   <- 1 - (ll_mod / ll_null)
R2_coxsnell   <- 1 - exp((ll_null - ll_mod) * 2 / n)
R2_nagelkerke <- R2_coxsnell / (1 - exp(2 * ll_null / n))

# --- 3. information criteria -------------------------------------------
aic <- AIC(nbfit)
bic <- BIC(nbfit)

# --- 4. summary table ---------------------------------------------------
fit_tbl <- data.frame(
  Metric = c("Log‑likelihood", "AIC", "BIC",
             "McFadden R-squared", "Cox–Snell R-squared", "Nagelkerke R²"),
  Value  = paste0(c(round(ll_mod, 1),
             round(aic, 1),
             round(bic, 1),
             round(R2_mcfadden*100,   1),
             round(R2_coxsnell*100,   1),
             round(R2_nagelkerke, 1)),"%")
)

knitr::kable(fit_tbl[-c(1,2,3,6),],
             caption = "Goodness‑of‑fit",
             align   = "lc",row.names = F) %>%
  kable_styling(full_width = FALSE, position = "center")


#-----------------------------------------------------------
# moran's I
library(spdep)
coords <- st_coordinates(d$nodes)
# k neighbours to each intersections
nb = knn2nb(knearneigh(coords, k=3))
lw = nb2listw(nb, style="W")
moran.test(d$df$acc, lw)
# significant value means positive spatial autocorrelation >>
# intersections near each other have more accident counts more than by random chance
localmoran(d$df$acc,lw) -> lm
moran.plot(d$df$acc,lw)

d$nodes %>% 
  mutate(Moran_Cluster = attr(lm, "quadr")[,1]) %>%
ggplot() +
  geom_sf(data = montreal, color = "grey80", size = 0.3) +
  geom_sf(
          aes(color = Moran_Cluster), 
          size = 1) +
  scale_color_manual(values = c("grey", "red", "blue", "orange", "green")) +
  labs(color = "Moran Category",
       title = "Local Moran Cluster with 3 Neighbours") +
  theme_bw() +
  theme(panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        axis.title       = element_blank(),
        axis.text        = element_blank(),
        axis.ticks       = element_blank(),
        axis.line        = element_blank())

#---------------------------------------------
# plot number of accidents per 10k pedestrians
# not sure what to make out of this!
# maybe ignor those with low accident count

summary(d$df$acc)
  d$df %>% 
  mutate(acc_per_100k = ceiling(d$df$acc / (d$df$pi+1e-17) * 1000)) %>%
  filter(acc_per_100k != 0) %>%
  filter(acc_per_100k > median(acc_per_100k)) %>% 
  arrange(desc(acc_per_100k))
  ggplot() +
  geom_sf(data = montreal, color = "grey80", size = 0.3) +
  geom_sf(
    aes(color = acc_per_100k), 
    size = 1) +
  scale_color_viridis_c() +
  labs(color = "add desc",
       title = "add title") +
  theme_minimal()