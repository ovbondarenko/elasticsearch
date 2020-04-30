## This is the first slide (1)
- We will create simple set of slides
- Let's analyse some data
- Create a figure from the data
- Create a tablelibrary(tidyverse) (2)mtcars1 <- mtcars %>% (3)
             head() 
mtcars1 # produces the first five rows in the form of a tablemtcars %>% (4)
  ggplot(aes(x = disp, y = mpg)) +
  geom_point() +
  geom_smooth(method = "loess", colour = "blue", se = FALSE) +
  geom_smooth(method = "lm", colour = "red", se = FALSE) +
  labs(x = "Engine size",
      y = "Miles per gallon",
      title = "Relationship between engine size and milage for cars") +
  theme_bw() +
  theme(panel.grid.major = element_blank(),
          panel.grid.minor = element_blank()) +
  ggsave("test5.png")


```python

```
