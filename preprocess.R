setwd("PJ")

# Loading title
title <- read.delim("title.tsv", na.strings="\\N", stringsAsFactors = FALSE)
# Loading akas
akas <- read.delim("akas.tsv", na.strings = "\\N", stringsAsFactors = FALSE)

# Procedure to drop specified string vector of columns from a table
dropcols <- function(table, cols){
  return(table[ , !(names(table) %in% cols)])
}

# sqldf library to apply complex sql querys to filter data
library(sqldf)

# drop records with no Release Date, genre or runtimeMinutes
title = title[!is.na(title$startYear),]
title = title[!is.na(title$runtimeMinutes),]
title = title[!is.na(title$genres),]
title <- subset(title, isAdult = 0)
title <- dropcols(title, c("isAdult"))
# drop records with release date later than 2022(current year)
title <- subset(title, startYear <= 2022)

# Selects movies that are newer than 2019
newMovies <- sqldf("select tconst from title where startYear > 2015 and titleType = 'movie'")

# Select distinct titles that are from USA,UK or India 
engRegionmovies <- sqldf("select distinct titleId from akas where region='US' or region='IN'")

# Select movie details, that satisfy the above two filters.
newEngRegionMovies <- sqldf("select Title.tconst from title, newMovies, engRegionmovies where engRegionmovies.titleId = title.tconst and newMovies.tconst = title.tconst")

# new final table saves the new data
final <- sqldf("select * from title, newEngRegionMovies where title.tconst = newEngRegionMovies.tconst")

# drop redundant columns
final <- dropcols(final, c("titleType", "endYear", "originalTitle"))
final <- dropcols(final, c("tconst.1"))
# final <- sqldf("select * from final where isAdult=0")

# removing temporary tables to clear workspace
rm(newMovies, engRegionmovies, newEngRegionMovies)


# final <- sqldf("select * from final where startYear <= 2022")




# import crew data
crew <- read.delim("crew.tsv", na.strings="\\N", stringsAsFactors = FALSE)

# filter crew data to relevant film titles based on final table.
crew <- sqldf("select crew.tconst, directors, writers from crew, final where final.tconst = crew.tconst")

# merge the table with final by tconst
final <- merge(x=final, y=crew, by="tconst")

# format crew such that directors and writers are lists not string
crew$directors = strsplit(crew$directors, ",")
crew$writers = strsplit(crew$writers, ",")

# import principal
principal <- read.delim("principal.tsv", na.strings="\\N", stringsAsFactors = FALSE)

# filter principal to relevant title based on final table and drop irrelevant columns
principal <- sqldf("select principal.tconst, nconst, category from principal, final where final.tconst = principal.tconst")



# make a huge distinct 1 column table with all the person codes that are relevant in the project

# strip directors down to list

# directors <- unlist(crew$directors)
# writers <- unlist(crew$writers)

relevantPeople <- append(unlist(crew$directors), unlist(crew$writers))
relevantPeople <- append(relevantPeople, unlist(sqldf("select distinct nconst from principal")))
relevantPeopleTable <- data.frame(relevantPeople)
# relevantPerson = append(directors, writers)
# relevantPerson = append(relevantPerson, pcp)
# relevant = data.frame(relevantPerson)

# reducing to unique values
relevantPeopleTable <- sqldf("select distinct relevantPeople from relevantPeopleTable")

# import person data
person <- read.delim("person.tsv", na.strings = "\\N", stringsAsFactors = FALSE)

# filtering person to relevant people
person <- sqldf("select * from person, relevantPeopleTable where relevantPeople = nconst")
person <- dropcols(person, c("relevantPeople"))

# Finding the unidentified people, who are not listed in the person table but are relevant
relevantPeople = unlist(relevantPeopleTable)
identified = unlist(person["nconst"])

# set difference computes the difference between the two lists and returns
unidentified = setdiff(relevantPeople, identified)
unidentified = data.frame(unidentified)

rm(relevantPeople,identified)
# we now have ppl who are undocumented, the movies with these cast and directors need to be removed.

# convert director and writer column in final to list form
final$directors = strsplit(final$directors, ",")
final$writers = strsplit(final$writers, ",")

# Vectorizable function computes whether the unidentified people are involved in a movie
checkCastPresence <- function(col, unidentified){
  for (elem in col){
    if(elem %in% unidentified){
      return(FALSE)
    }
    else{
      return(TRUE)
    }
  }
}
# Vectorizing the function
vecfunc = Vectorize(checkCastPresence, vectorize.args = "col")

#unidentified = unlist(unidentified)
#final$isValid = vecfunc(final$directors, unidentified = unidentified)
#final <- final[final$isValid == TRUE, ]
#final$isValid = vecfunc(final$writers, unidentified = unidentified)
#final <- final[final$isValid == TRUE, ]
#final <- dropcols(final, c("isValid"))

crew$isValid = vecfunc(crew$directors, unidentified = unidentified)
crew <- crew[crew$isValid == TRUE, ]
crew$isValid = vecfunc(crew$writers, unidentified = unidentified)
crew <- crew[crew$isValid == TRUE, ]
crew <- dropcols(crew, c("isValid", "writers", "directors"))

crew <- data.frame(crew)
#final <- sqldf("select final.tconst, primaryTitle, startYear, runtimeMinutes, genres, directors, writers from final, crew where crew.nconst = final.nconst")

final <- subset(final, (tconst %in% crew))
final = final[!is.na(final$directors),]
final = final[!is.na(final$writers),]
# This filters the movies containing cast which are not identified.
titleswithUnidentifiedCast = sqldf("select distinct tconst from unidentified, principal where unidentified.unidentified = principal.nconst")
final <- subset(final, !(tconst %in% unlist(titleswithUnidentifiedCast)))

# We have the perfect final Dataset
# final processing to convert from lists to string
stringfromvector <- function(col){
  return(toString(col))
}
vectorizedstrfromvector <- Vectorize(stringfromvector, vectorize.args = "col")
final$directors = vectorizedstrfromvector(final$directors)
final$writers = vectorizedstrfromvector(final$writers)


# output to csv
write.csv(person, file = "people.csv", row.names = FALSE)
write.csv(final, file = "final.csv", row.names = FALSE)
# # YOU ARW HERER


# ratings <- read.delim("ratings.tsv", na.strings = "\\N", stringsAsFactors = FALSE)
# ratings <- sqldf("select * from ratings, final where ratings.tconst = final.tconst")

