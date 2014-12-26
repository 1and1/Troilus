package com.unitedinternet.troilus.userdefinieddatatypes;


import com.unitedinternet.troilus.Field;


public class Score {

    @Field(name = "score")
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
