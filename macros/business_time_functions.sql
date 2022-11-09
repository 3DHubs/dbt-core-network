{# 
Ideas coming from:
https://github.com/dbt-labs/dbt-labs-experimental-features/tree/main/business-hours
https://docs.getdbt.com/blog/measuring-business-hours-sql-time-on-task

JG 2022-11-08 Due performance issues decided to use nested macro's instead of subqueries. Subquery is supposed to give more flexibility.
#}


--------- using macros

{% macro weekdays_between(start_date, end_date) %}

        datediff('day', {{ start_date }}, {{ end_date }} ) -
        datediff('week', {{ start_date }}, dateadd('day', 1, {{ end_date }} )) -
        datediff('week', {{ start_date }}, {{ end_date }} )

{% endmacro %}


{# non_business_hours_between:

    Terms in this macro:
    - weekdays_between:
        returns the number of weekdays between two dates. This is used to evaluate the number of overnights that occur between the two dates.
        i.e. Monday to Wednesday is 2 weekdays, ie two weeknights of non-business time. Friday to Monday evaluates to one overnight (8pm-12am Fri + 12am-8am Monday)
        we multiply by 12 to convert the weekdays between to hours.
    - evaluate weekends:
        in order to compare if a weekend falls in between two dates, we can compare the regular datediff to the weekday datediff.
        the difference is the number of weekend days (example, Friday to Monday, Datediff = 3, weekday = 1, 3-1 = 2)
        muliply the difference by 24 hours per weekend day

 #}

{% macro non_business_hours_between(start_date, end_date) %}
    {% set non_working_hours = (24 - (  var("working_hour_end")  -  var("working_hour_start") ))  %}
    
        coalesce(
            (( {{ weekdays_between(start_date, end_date) }} ) *  {{ non_working_hours }}  )
                + ((datediff('day', {{ start_date }}, {{ end_date }} )
                - ({{ weekdays_between(start_date, end_date) }})
            ) * 24 )::int,
            0
        )

{% endmacro %}


{# 
    
    business_minutes_between:
        This macro leverages the above macros to remove non-business time from the calculation of time durations. 
        
        the basic structure here is:
            (date diff in minutes) - (non-business hours * 60) = business minutes

 #}


{% macro business_minutes_between (start_date, end_date) %}

        datediff('minute', {{ start_date }}, {{ end_date }} )
            - ( {{ non_business_hours_between( start_date, end_date ) }} * 60 )

{% endmacro %}