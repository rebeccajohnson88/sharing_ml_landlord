import plotnine
from postgres_functions import *


## plot feature importances
def plot_feature_imp(model_id, alchemy_connection,
                    filter_to_top = False,
                    rank_threshold = 20):

    ## query to pull model id's data
    feature_importance_query = """
    select * 
    from dssg_results.feature_importances
    where model_id = {model_id}
    """.format(model_id = model_id)
    feature_importance_data = readquery_todf_postgres(sqlalchemy.text(feature_importance_query),
                                     alchemy_connection)

    ## sort values by feature importance
    sorted_onemodel = feature_importance_data.sort_values(by = 'feature_importance', ascending = True).copy()
    sorted_onemodel['feature_clean'] = sorted_onemodel.feature.str.replace("_", " ").str.title()

    ## order the features
    order_features = list(sorted_onemodel.feature_clean)

    ## create categorical version for graph
    sorted_onemodel['feature_categorical'] = sorted_onemodel.feature_clean.astype('category',
                                    ordered = True,
                                    categories = order_features)
    
    ## filter
    if filter_to_top == True:
        sorted_onemodel_toplot = sorted_onemodel[sorted_onemodel.rank_abs < rank_threshold].copy()
        importance_graph = (ggplot(sorted_onemodel_toplot, aes(x = 'feature_categorical',
                          y = 'feature_importance')) +
         geom_bar(stat = 'identity', fill = '#458B74', alpha = 0.2, color = 'black')  +
         xlab('Feature') +
        ylab(str("Feature Importance \n model_id:") +
                  str(feature_importance_data.model_id.iloc[0])) +
         coord_flip())
        return(sorted_onemodel_toplot, importance_graph)
    
    else:
        importance_graph = (ggplot(sorted_onemodel, aes(x = 'feature_categorical',
                          y = 'feature_importance')) +
         geom_bar(stat = 'identity', fill = '#458B74', alpha = 0.2, color = 'black')  +
         xlab('Feature') +
         ylab(str('Feature Importance \n(' +
                  str(mg_row.algorithm_type.iloc[0]) + "; model_id: " +
                  str(feature_importance_data.model_id.iloc[0]) +
                 ")")) +
         coord_flip() +
         theme)
        return(sorted_onemodel, importance_graph)
    
