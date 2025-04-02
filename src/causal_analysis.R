source("./load_libs.R")

dat <- readRDS("../data/processed_data_1033.rds")

# if (!inherits(dat$Adj, "dgCMatrix")) dat$Adj <- as(dat$Adj, "dgCMatrix")
# rownames(dat$Adj) <- levels(dat.df$node_id)
# colnames(dat$Adj) <- levels(dat.df$node_id)
# step 1: decide which variables to use for the propensity model

dat$df %>%
  as.data.frame() %>%
  select(tot_crossw, acc, node_id,
         total_lane, number_of_, of_exclusi, any_exclus, #intersection size
         fi, pi, # traffic flow
         parking, median, # road design
         all_pedest, ped_countd, half_phase, lt_protect, #pedest. protection
         distdt, borough,  #neighborhood
         commercial) %>% 
  mutate(borough = as.factor(borough),
         parking = as.factor(parking),
         node_id = as.factor(node_id)) ->
  dat.df

# step 2: use MRF smooth model
library(mgcv)
m1 <- gam(
  acc ~ tot_crossw  + s(node_id, bs="mrf", xt=list(adj=dat$Adj)),
  data = dat.df,
  family = poisson()
)

library(INLA)

m2 <- inla(
  acc ~ tot_crossw + f(node_id, model="besag", graph = dat$Adj),
  data = dat.df,
  family = "poisson"
)

# step 2:  fit propensity model


# step 3: fit the causal model

penalized_graph_solver <- function(Y, X, L, lambda){
  n <- nrow(Y)
  I <- diag(1, n, n)
  B <- solve(I + lambda * L)
  K.partial = solve(t(X)%*%X) %*% t(X)
  K = X %*% K.partial
  IminusK = I - K
  U = MASS::ginv(IminusK) %*% B %*% IminusK %*% Y
  Theta = K.partial %*% (Y-U)
  return(list(U=U, Theta=Theta))
}

res <- penalized_graph_solver(
  Y = log1p(dat.df %>% select(acc)) %>% as.matrix(),
  X = dat.df %>% select(tot_crossw, median) %>% as.matrix(),
  L = as.matrix(diag(rowSums(A)) - dat$Adj),
  lambda = 1
)

plot((res$Theta[1]*dat.df$tot_crossw), log1p(dat.df$acc))
plot((res$Theta[2]*dat.df$median), log1p(dat.df$acc))
plot(res$U, log1p(dat.df$acc))


dat.df %>%
  mutate(acc_log_f = as.factor(round(log1p(acc),2))) %>% 
  ggplot(aes(x = acc_log_f, y = tot_crossw)) +
  geom_boxplot()

dat.df %>%
  mutate(acc_log_f = as.factor(round((acc),2)),
         tot_crossw = tot_crossw * res$Theta[1]) %>%
  ggplot(aes(x = acc_log_f, y = tot_crossw)) +
  geom_boxplot()