package com.ascm.pacs.core.config;

import javax.inject.Inject;

import org.hibernate.SessionFactory;
import org.springframework.context.annotation.Configuration;

@Configuration
public final class CoreConfig {
	@Inject
	private SessionFactory sessionFactory;

}
