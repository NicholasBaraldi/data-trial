--pensando nas queries:

select name, avg(rating) as avg_rating from customer_reviews_google
group by name
order by avg_rating desc
limit 100

select cpg.name, round(avg(cpg.rating)::numeric, 2) as avg_rating from company_profiles_google_maps cpg
join customer_reviews_google crg on cpg.name = crg.name
where crg.reviews > 5 --usar crg.review
group by cpg.name
order by avg(cpg.rating) desc

----------------------------------------------------------------------------------------------------------------------

--staging do andre:

create table staging.tabela as
select
    coluna1,
    coluna3 as renomeada,
    coluna4::date,
    coluna5
from raw.tabela
where coluna5 is not null

----------------------------------------------------------------------------------------------------------------------

--descobertas:

select name from company_profiles_google_maps c
join fmcsa_companies f on c.name = f.company_name -- só UMA empresa se relaciona - descartar fmcsa_companies

-- parece q todos outros datasets sao um lixo, usar só os do google num primeiro momento

