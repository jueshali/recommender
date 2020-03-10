# recommender
电影推荐系统包含四大模块。

## 数据加载模块

用于加载movieLen数据集和电影的tags数据集，在es中加载用于未来前端的使用，在mongo加载数据用于分析。

## 离线电影推荐

### 历史统计推荐
1. 电影topN
2. 电影最近topN
3. 各电影平均评分
4. 每个电影类别topN

### 用户个性化推荐
- 根据movieLen数据集用户的评分数据获取（uid,mid,score）元组，继续使用ALS算法训练，最后预测。
- ALS算法可以提取出电影的特征信息，可以计算各个电影的相似度。

## 实时电影推荐

- 从用户当前评分电影的最相似电影集合中选择电影推荐，推荐的score由用户最近的k个评分电影获得。

## 基于内容的电影推荐
- IF-IDF获取电影特征。计算相似度

