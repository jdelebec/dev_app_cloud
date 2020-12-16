//1
db.getCollection("people_degree").explain(true).distinct("subject",{"institution":"Harvard Business School"})



//2
db.getCollection("object_office").explain(true).find({ 
    "state_code_offices":"CA", "entity_type": "Company", 
    "description_offices": /Headquarters/i })



//3
opLookUp = {
    $lookup: {
        "from": "object_office",
        "localField": "object_id",
        "foreignField": "id",
        "as": "object_office"
    }}
 
opProject = {
    $project: {
        "raised_amount_usd": 1,
        "funding_round_id": 1,
        "object_id": 1,
        "object_office.name" : 1,
        "object_office.id" : 1
    }}
    
opMatch = {
    $match: {
        "raised_currency_code": "USD",
        "funded_at": /2010/i,
        "raised_amount_usd": {
            "$gt": 50000000,
            "$not": { "$lte": 50000000.0 }
        }}}

db.getCollection("fundings_rounds").explain(true).aggregate([opMatch, opProject])


//4
opLookUp = {
$lookup: {
             "from": "relationships",
            "localField": "object_id",
            "foreignField": "person_object_id",
            "as": "relation"      
    }}

opMatch = {
$match: {
        "affiliation_name": "Facebook"
    }}

opProject = {
$project: {
        "facebook_company" : 
        "$relation.relationship_object_id",
        "first_name" : 1,
        "last_name" : 1,
        "object_id":1,
        "_id" : 0
    }}

opOut = { $out: "facebook_relationship" }

db.getCollection("people_degree").aggregate([opLookUp,opMatch, opProject]);

company = db.getCollection("facebook_relationship").distinct("facebook_company");

db.getCollection("object_office").find({"id" : {"$in" : company},"state_code_object" : "CA"},{"name" :1, "id" :1 ,"state_code_offices" : 1});



//5

opProject = {
$project: {
        "affiliation_name": 1,
        "institution": 1,
    }}

opGroup = {
$group: {
        _id : "$affiliation_name", total : {$sum : 1}
    }}

opSort = {
$sort: { total: -1 }  
};

db.getCollection("people_degree").explain(true).aggregate([opProject,opGroup,opSort])


//6
db.getCollection("object_office").explain(true).aggregate({$group:{_id:"$city_offices",nombre:{$sum:+1}}})


//7

opLookUp = {
    $lookup: {
        "from": "object_office",
        "localField": "object_id",
        "foreignField": "id",
        "as": "object_office"
    }}
 
opProject = {
    $project: {
        "raised_amount_usd": 1,
        "funding_round_id": 1,
        "object_id": 1,
        "object_office.name" : 1,
        "object_office.id" : 1,
        "object_office.city_object":1,
        "object_office.country_code_offices":1

    }}

opMatch = {
    $match: {
        "raised_amount_usd": {
            "$gt": 7540355*(1.3),
            "$not": { "$lte": 7540355*(1.3) }
        },
        "raised_currency_code":"EUR",
        } }

db.getCollection("fundings_rounds").explain(true).aggregate([ opMatch, opProject])

//8
opProject = {
	$project: {
	"country_code_object": 1,
	"funding_total_usd" : 1,
	"total" : 1,
	 year : {$year : "$last_funding_at"}
	}} 

opGroup = {
	$group: {
    	_id : {year: "$year" , pays : "$country_code_object"} ,
    	fonds : {$sum: "$funding_total_usd"}	,	
	}}

opSort = {
	$sort: { fonds: -1 } 
}

db.getCollection("object_office").explain(true).aggregate([opProject,opGroup,opSort])
