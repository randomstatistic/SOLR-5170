
About
==========

I was getting tired of building a custom solr using the patch found at [SOLR-5170](https://issues.apache.org/jira/browse/SOLR-5170)

The files involved in the patch are pretty free-standing though, so this project allows you to build a jar containing the
functionality of that patch, and add that jar to your solr config the same as any other plugin you might wish.

This allows you to use the stock Solr distribution and treat this as a standard plugin.

Test infrastructure files were copied in from the solr source so that tests can be run.

Building
==========

   mvn package
   
Then put the resulting jar someplace Solr can get it.   
    
