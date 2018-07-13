package com.qc.itaojin.canalclient;

import com.qc.itaojin.canalclient.entity.DemoEntity;
import com.qc.itaojin.exception.ItaojinHBaseException;
import com.qc.itaojin.service.IHBaseService;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class CanalClientApplicationTests {

	@Autowired
	private IHBaseService hBaseService;

	@Test
	public void contextLoads() throws ItaojinHBaseException {

		List<DemoEntity> list =  hBaseService.scanAll(DemoEntity.class);
		for (DemoEntity demoEntity : list){
			System.out.println(demoEntity);
		}

	}

}
