**Effect of Sample Size on Expected Mean and Standard Deviation**

Using Apache Spark, Scala, and Zeppelin

- **Problem statement:**

This is a study of some aspects of the Central Limit Theorem, which states that &quot;Said another way, CLT is a statistical theory stating that given a sufficiently large sample size from a population with a finite level of variance, the mean of all samples from the same population will be approximately equal to the mean of the population. Furthermore, all the samples will follow an approximate [normal distribution](https://www.investopedia.com/terms/n/normaldistribution.asp) pattern, with all variances being approximately equal to the [variance](https://www.investopedia.com/terms/v/variance.asp) of the population, divided by each sample&#39;s size&quot; [[investopedia](https://www.investopedia.com/terms/c/central_limit_theorem.asp)] .

The approach I followed was taking a specific fraction of the population, and take a large number of random samples, with replacement, from that fraction, and calculate the average of the samples&#39; means and standard deviations. Then repeated the experiment many times with increasing fraction sizes.

The fraction size here represents the sample size, and the average samples&#39; mean represents the expected population mean based on the samples.

By measuring the difference between the population mean and the average means corresponding to each sample size, we can see the effect of increasing the sample size on the accuracy of the expected population mean. The same thing applies to the standard deviation.

I hope the above made any sense!

- **Dateset** :
  - &quot;City of Chicago employees.&quot; [Take a look](https://data.cityofchicago.org/Administration-Finance/Current-Employee-Names-Salaries-and-Position-Title/xzkq-xp2w)!
  - We&#39;re interested only on two variables, the two holding the department and the annual salary. We run the experiment on each department separately and do a comparison by the end.

- **Technologies:**
  - The mighty &quot;Apache Spark&quot;.
  - The beautiful &quot;Scala&quot;.
  - The promising &quot;Apache Zeppelin&quot;.

- **Final Results:**

![](RackMultipart20200615-4-oodys5_html_21d3836e379c6a1c.png)
