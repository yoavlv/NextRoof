import matplotlib.pyplot as plt

def result_plot(scores):
    res_scores = []
    model_names = []

    for model_name, model_scores in scores.items():
        res_scores.append(scores[model_name]['results_score'])
        model_names.append(model_name)
    fig, ax = plt.subplots(figsize=(12, 8))
    ax.boxplot(res_scores, labels=model_names, showmeans=True)
    ax.set_xlabel('Model')
    ax.set_ylabel('Accuracy (Mean)')
    ax.set_title('Model Performance Comparison')
    plt.savefig("C:/Users/yoavl/NextRoof/img/result_plot.png")
    plt.close(fig)

def plot_model_scores(scores):
    r2_scores = []
    mae_scores = []
    model_names = []

    for model_name, model_scores in scores.items():
        r2_scores.append(model_scores['r2_score'])
        mae_scores.append(model_scores['mae_score'])
        model_names.append(model_name)

    fig, (ax1, ax2) = plt.subplots(ncols=2, figsize=(25, 5))
    ax1.bar(model_names, r2_scores, width=0.2, color='Red')
    ax1.set_xlabel('R2 Score')
    ax1.set_title('Model R2 Scores')

    ax2.bar(model_names, mae_scores, width=0.15)
    ax2.set_xlabel('MAE Score')
    ax2.set_title('Model MAE Scores')
    plt.savefig("C:/Users/yoavl/NextRoof/img/plot_model_scores.png")
    plt.close(fig)