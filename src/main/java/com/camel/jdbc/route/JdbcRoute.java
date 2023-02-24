package com.camel.jdbc.route;

import com.camel.jdbc.dao.Employee;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class JdbcRoute extends RouteBuilder {

    @Autowired
    DataSource dataSource;

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void configure() throws Exception {

        from("{{timeRoute}}")
                .routeId("jdbcRoute")
                .to("direct:select");

        from("direct:select").setBody(constant("select * from Employee")).to("jdbc:dataSource")
                .process(new Processor() {
                    public void process(Exchange xchg) throws Exception {
                        ObjectMapper obj = new ObjectMapper();
                        //the camel jdbc select query has been executed. We get the list of employees.
                        ArrayList<Map<String, String>> dataList = (ArrayList<Map<String, String>>) xchg.getIn()
                                .getBody();
                        List<Employee> employees = new ArrayList<Employee>();
                        System.out.println("emps : " + dataList);
                        for (Map<String, String> data : dataList) {
                            Employee employee = new Employee();
                            employee.setEmpId(Integer.valueOf(data.get("ID")));
                            employee.setEmpName(data.get("EMP_NAME"));
                            employee.setDepartment(data.get("DEPARTMENT"));
                            employee.setSalary(Long.valueOf(data.get("EMP_SALARY")));
                            employees.add(employee);
                        }
                        xchg.getIn().setBody(obj.writeValueAsString(employees));

                    }
                })
                .log(LoggingLevel.INFO, "json :: ${body}")
        ;
    }
}
