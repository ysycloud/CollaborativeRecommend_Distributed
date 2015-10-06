# CollaborativeRecommed_Distribution

# As one of the most successful approaches to building recommender systems, 
Collaborative Filtering (CF) uses the known preferences of a group of users 
to make recommendations and predictions of the unknown preferences for other users. 
It presents two main kinds of CF techniques: user-based and item-based. However, 
the architecture of traditional recommendation system is usually with centralized 
stand-alone node, which is not suitable for large-scale data analysis and processing 
due to its limited processing ability and poor scalability. The main purpose of this 
thesis is to implement two kinds of CF algorithms based on MapReduce programming 
framework to improve the efficiency of traditional CF algorithms.

# The designing section of this thesis analyzes CF algorithm, and implements 
the parallelization for calculating the similarity and the matrix multiplication 
operation based on MapReduce. In comparison with the traditional serial algorithm, 
the calculation of the similarity has shown significant performance advantages. 
The matrix multiplication shows a better scalability. And the performance advantage 
will gradually reflect when the number of nodes is appropriate. At the same time, 
the parallelism of TopK-based and threshold-based filtering policies further 
enhances the overall performance of the algorithm. Moreover, this thesis makes 
use of the iterative characteristics of Spark to optimize the user-based CF algorithm, 
by combining PeopleRank algorithm, a typical method to measure the user importance 
of social network. To some extent, the recommended effect has been improved. 
