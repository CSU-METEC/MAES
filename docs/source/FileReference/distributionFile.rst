.. _distribution_file_label:

MEET Distribution File Format
=============================

.. figure: graphics/DistributionFile.png

   Example Distribution File
   
A MEET distribution file is a .csv file with metadata attached.  The special tag %%%ENDOFMETADATA%%% separates the two sections.  The only metadata field used directly by MEET is the 'Distribution Type' field, which describes the format of the actual data which is found directly following the %%%ENDOFMETADATA%%% tag.  Other metadata fields are for human and other tool reference.

Valid values for the Distribution Type field are:

* **Histogram**  A Histogram distribution is defined by observed data.  Columns of the Histogram files are **<value>** which defines the numeric value (exact name of the value is defined by the use of the Distribution file), and **Probability** which defined the probability that the numeric value will be chosen.
* **Constant**  A Constant distribution always returns the value of the first column of the data.
* **Normal**  A Normal distribution will return a number in the range defined by **mu**, the mean of the value and **sigma**, the standard distribution of the value.
* **Lognormal** A Lognormal distribution will return a value with a logarithm in the Normal distribution defined by **mu** and **sigma**
* **Triangular** A Triangular distribution will return a value on a triangle defined by **min**, minimum value, **mean**, mean value, and **max**, max value.
* **Uniform** A Uniform distribution will return a random number in the range [**min**, **max**)
