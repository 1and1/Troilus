package com.unitedinternet.troilus.userdefinieddatatypes;

import javax.persistence.Column;


public class Score {

    @Column(name = "score")
    private Integer score;
        
    
    @SuppressWarnings("unused")
    private Score() {  }
    
    public Score(Integer score) {
        this.score = score;
    }

    public Integer getScore() {
        return score;
    }
}
