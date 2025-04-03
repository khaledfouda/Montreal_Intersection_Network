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


#-------------------------------------------------------------
lmfit <- glm("acc ~ . +1 + tot_crossw - node_id - borough",
             data=dat.df, family="poisson")
summary(lmfit)
par(mfrow=c(2,2))
plot(lmfit)
drop1(lmfit, test="Chisq")
anova(lmfit, test = "Chisq") %>% as.data.frame() %>%
  arrange(desc(Deviance))
mcfadden_R2 <- 1 - lmfit$deviance / lmfit$null.deviance
pred <- predict(lmfit, type="response")
ss_res <- sum((dat.df$acc - pred)^2)
ss_tot <- sum((dat.df$acc - mean(dat.df$acc))^2)
r2 <- 1 - ss_res / ss_tot
rmse <- sqrt(mean((dat.df$acc - pred)^2))
#----------------------------------------------------------------

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
lambda_fit <- function(Y, X, L, lambda = seq(10,50,length.out=40)){
  low.rmse <- Inf
  best.lambda <- NA
  for(l in lambda){
    res = penalized_graph_solver(Y, X, L, l)
    pred = (X %*% res$Theta )
    rmse <- sqrt(mean((Y - pred)^2))
    print(paste(l, " - ",rmse))
    if(rmse <= low.rmse){
      low.rmse = rmse
      best.lambda = l
    }
  }
  return(c(best.lambda, low.rmse))
}

Y = log1p(dat.df %>% select(acc)) %>% as.matrix()
X = dat.df %>% select(tot_crossw, median) %>% as.matrix()
L = as.matrix(diag(rowSums(dat$A)) - dat$Adj)

ll <- lambda_fit(Y, X, L)
ll
res <- penalized_graph_solver(Y, X, L, ll[1])
pred = expm1(X %*% res$Theta  + res$U)
rmse <- sqrt(mean((dat.df$acc - pred)^2))
ss_res <- sum((dat.df$acc - pred)^2)
ss_tot <- sum((dat.df$acc - mean(dat.df$acc))^2)
r2 <- 1 - ss_res / ss_tot
res$Theta


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

dat.df %>%
  mutate(acc_log_f = as.factor(round((acc),2)),
         tot_crossw = expm1(res$U)) %>%
  ggplot(aes(x = acc_log_f, y = tot_crossw)) +
  geom_boxplot()