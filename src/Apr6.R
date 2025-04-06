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

# corr central
cor_m <- cor(d$nodes %>% as.data.frame() %>% select(-geometry, -node.id,  contains("unweighted")))
cor_m[abs(cor_m)<.3] = NA
ggcorrplot::ggcorrplot(cor_m,lab = T,type = "upper", insig="blank",
                       title = "Correlation Coefficients (>.3) between Centrality Measures",
                       show.diag = F, show.legend = F)
# corr cov:



cor_m <- cor(d$dfs  %>% select( -node_id, -borough))
cor_m[abs(cor_m)<.3] = NA
ggcorrplot::ggcorrplot(cor_m,lab = T,type = "upper", insig="blank",
                       title = "Correlation Coefficients (>.3) between features",
                       show.diag = F, show.legend = F)

#----------
# prepare final dataset
d$dfs %<>%
  select( - of_exclusi) %>%
  rename(node.id = node_id) %>%
  left_join(
    d$nodes %>%
      as.data.frame() %>%
      select(node.id, deg_unweighted, deg_weighted, betweenes, closeness, eigcentrality) %>%
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
       title = "Intersections colored by Assigned Clusters") +
  theme_minimal()

# the following part is only to confirm that the clusters are good. 
set.seed(2025); d$dfs.scaled %>%
  select(-node.id, -borough) %>% select(contains("centr_")) %>%
  prcomp() -> prf
data.frame(prf$x[,1:2], cluster=as.factor(kmfit$cluster)) %>%
  ggplot(aes(PC1, PC2, color=cluster)) +
  geom_point(alpha=0.7)

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
  select( - node.id, -borough, -centr_eigcentrality) %>%
  glm(formula= acc ~ ., family="poisson") -> poisfit
summary(poisfit)
# dispersion test
AER::dispersiontest(poisfit) 

d$dfs.scaled %>%
  select( - node.id, -borough,-clusterA, -clusterB, -contains("centr_")) %>%
  MASS::glm.nb(formula=acc ~ .) %>% summary()

d$dfs.scaled %>%
  select( acc, contains("centr_"), -centr_eigcentrality) %>%
  MASS::glm.nb(formula=acc ~ .) %>% summary()

d$dfs.scaled %>%
  select(clusterA, clusterB, acc) %>%
  MASS::glm.nb(formula=acc ~ .) %>% summary()

d$dfs.scaled %>%
  select( - node.id, -borough, -centr_eigcentrality) %>%
  MASS::glm.nb(formula=acc ~ .) %>% summary()

d$dfs.scaled %>%
  select( - node.id, -borough, -contains("centr_")) %>%
  MASS::glm.nb(formula=acc ~ .) %>% summary()


# model with sig covariates only <unnormalized so we can interpret them>
d$dfs %>%
  mutate(clusterA = as.factor(d$nodes$clusterA)) %>%
  select(acc, all_pedest, fi, pi, half_phase, commercial,
         clusterA) %>%
  MASS::glm.nb(formula=acc ~ . + pi:clusterA  ) -> nbfit
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
broom::tidy(nbfit2, exponentiate = TRUE) %>%
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
#-----------------------------------------------------------
