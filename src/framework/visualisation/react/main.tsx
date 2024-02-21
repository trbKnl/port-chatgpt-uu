import * as React from "react";

interface MainProps {
  elements: JSX.Element[];
}

export const Main = ({ elements }: MainProps): JSX.Element => {
  elements = elements.map((element, index) => {
    return { ...element, key: `${index}` };
  });

  if (process.env.REACT_APP_BUILD !== "standalone" && process.env.NODE_ENV === "production") {
    return <Embedded elements={elements} />;
  } else {
    return <Standalone elements={elements} />;
  }
};

const Embedded = ({ elements }: MainProps): JSX.Element => {
  return <div className="max-w-7xl w-full h-full">{elements}</div>;
};

const Standalone = ({ elements }: MainProps): JSX.Element => {
  return (
    <div className="p-4 sm:p-8 md:p-12 flex justify-center w-full h-full">
      <div className="max-w-7xl">{elements}</div>
    </div>
  );
};
