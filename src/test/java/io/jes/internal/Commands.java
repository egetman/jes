package io.jes.internal;

import java.beans.ConstructorProperties;

import io.jes.Command;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

public class Commands {

    @ToString
    @EqualsAndHashCode
    public static class SampleCommand implements Command {

        @Getter
        private final String name;

        @ConstructorProperties("name")
        public SampleCommand(String name) {
            this.name = name;
        }
    }

}
