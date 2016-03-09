package com.stacksync.syncservice.db.hibernateOGM;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;


public class HibernateOGMEntityManagerFactory {
        public static EntityManagerFactory getEntityManagerFactory(){
            return Persistence.createEntityManagerFactory("org.hibernate.ogm.tutorial.jpa");
        }
}
