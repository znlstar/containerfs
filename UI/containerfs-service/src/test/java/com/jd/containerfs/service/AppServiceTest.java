package com.jd.containerfs.service;

import org.junit.runner.RunWith;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;

/**
 * Created by lixiaoping3 on 17-11-14.
 */
@ContextConfiguration(locations = {"classpath:/spring/spring-config.xml"})
@ActiveProfiles("test")
@RunWith(SpringJUnit4ClassRunner.class)
public class AppServiceTest {



}
